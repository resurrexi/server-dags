from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.utils.dates import days_ago


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
        return "download"

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
        from fastanime.libs.common.mini_anilist import get_basic_anime_info_by_title
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
        ti.xcom_push(key="episodes", value=episodes)

        # normalize titles
        anilist_anime_info = get_basic_anime_info_by_title(fetched_anime["title"])
        ti.xcom_push(key="anilist_anime_info", value=anilist_anime_info)

    @task
    def download_episode(**context):
        from fastanime.constants import USER_VIDEOS_DIR

        # TODO: get episode top stream link
        # TODO: get normalized episode title using `anilist_anime_info`
        # TODO: get preferred subtitle url
        # TODO: download episode
        return USER_VIDEOS_DIR

    show_options = show_options()
    get_episodes = get_episodes()

    (
        search_for_anime()
        >> parse_results()
        >> (
            show_options,
            get_episodes,
        )
    )
    show_options >> get_episodes >> download_episode()


download_anime()
