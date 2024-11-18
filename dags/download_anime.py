from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.utils.dates import days_ago

_SERVER = "Yt"


@dag(
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "anime": Param(
            type="string",
            title="Anime Title",
            description="Enter the name of the anime.",
        )
    },
    max_active_tasks=3,
)
def download_anime():
    @task
    def search_for_anime(**context):
        from fastanime.AnimeProvider import AnimeProvider

        anime = context["params"]["anime"]
        provider = AnimeProvider("allanime")

        search_results = provider.search_for_anime(
            anime,
            translation_type="sub",
        )

        return search_results

    @task.branch
    def parse_results(**context):
        ti = context["ti"]

        task_xcom = ti.xcom_pull(task_ids="search_for_anime", key="return_value")
        results = task_xcom["results"]

        if len(results) > 1:
            ti.xcom_push(key="anime_options", value=results)
            return "show_options"

        ti.xcom_push(key="anime_title", value=results[0]["title"])
        ti.xcom_push(key="episode_count", value=results[0]["availableEpisodes"]["sub"])
        return "get_episodes"

    @task
    def show_options(**context):
        ti = context["ti"]

        options = ti.xcom_pull(task_ids="parse_results", key="anime_options")
        parsed_options = [
            {"id": result["id"], "title": result["title"]} for result in options
        ]

        return parsed_options

    @task(trigger_rule="one_success")
    def get_episodes(**context):
        from fastanime.AnimeProvider import AnimeProvider
        from fastanime.Utility.data import anime_normalizer
        from thefuzz import fuzz

        ti = context["ti"]
        anime = context["params"]["anime"]

        task_xcom = ti.xcom_pull(task_ids="search_for_anime", key="return_value")
        results = task_xcom["results"]
        results_ = {result["title"]: result for result in results}
        closest_match = max(
            results_.keys(),
            key=lambda title: fuzz.ratio(
                anime_normalizer.get(title, title),
                anime,
            ),
        )

        provider = AnimeProvider("allanime")
        fetched_anime = provider.get_anime(results_[closest_match]["id"])

        if not fetched_anime:
            raise AirflowFailException("Failed to find anime")

        # get episodes
        episodes = sorted(
            fetched_anime["availableEpisodesDetail"]["sub"],
            key=float,
        )

        ti.xcom_push(key="anime_id", value=fetched_anime["id"])
        ti.xcom_push(key="anime_title", value=fetched_anime["title"])

        return episodes

    @task(map_index_template="{{ task.episode }}")
    def download_episode(episode, get_episodes_task_id="get_episodes", **context):
        from fastanime.AnimeProvider import AnimeProvider
        from fastanime.cli.utils.utils import (
            filter_by_quality,
            move_preferred_subtitle_lang_to_top,
        )
        from fastanime.constants import USER_VIDEOS_DIR
        from fastanime.Utility.downloader.downloader import downloader

        ti = context["ti"]
        anime_id = ti.xcom_pull(task_ids=get_episodes_task_id, key="anime_id")
        anime_title = ti.xcom_pull(task_ids=get_episodes_task_id, key="anime_title")

        # get stream link
        provider = AnimeProvider("allanime")
        # need to set new instance provider's store
        provider.anime_provider.store.set(
            anime_id, "anime_info", {"title": anime_title}
        )
        streams = provider.get_episode_streams(anime_id, episode, "sub")
        if not streams:
            raise AirflowFailException("Failed to get streams")

        servers = {server["server"]: server for server in streams}
        stream_link = filter_by_quality("1080", servers[_SERVER]["links"])
        if not stream_link:
            raise AirflowFailException("No streams found")

        link = stream_link["link"]
        provider_headers = servers[_SERVER]["headers"]
        subtitles = servers[_SERVER]["subtitles"]
        episode_title = f"{anime_title}; Episode {str(episode).zfill(2)}"

        # get preferred subtitle url
        subtitles = move_preferred_subtitle_lang_to_top(subtitles, "eng")

        downloader._download_file(
            link,
            anime_title,
            episode_title,
            USER_VIDEOS_DIR,
            True,
            vid_format="best[height<=1080]/bestvideo[height<=1080]+bestaudio/best",
            force_unknown_ext=True,
            verbose=False,
            headers=provider_headers,
            sub=subtitles[0]["url"] if subtitles else "",
            merge=True,
            clean=True,
            prompt=False,
        )

    show_options = show_options()
    get_episodes = get_episodes()
    download_episodes = download_episode.expand(episode=get_episodes)

    (
        search_for_anime()
        >> parse_results()
        >> (
            show_options,
            get_episodes,
        )
    )
    show_options >> get_episodes >> download_episodes


download_anime()
