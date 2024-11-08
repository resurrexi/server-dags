import json

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago

_ALLANIME_TEMPLATE = "allmanga.to/bangumi/{anime_id}"


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
    @task.bash(output_processor=lambda x: json.loads(x))
    def search_for_anime(**context):
        anime = context["params"]["anime"]

        return f"fastanime grab -t '{anime}' --search-results-only"

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

    @task.bash
    def download(**context):
        ti = context["ti"]

        anime = ti.xcom_pull(task_ids="parse_results", key="anime_title")

        return f"fastanime download -t '{anime}'"
    
    download = download()

    (
        search_for_anime()
        >> parse_results()
        >> (
            show_options() >> download,
            download,
        )
    )


download_anime()
