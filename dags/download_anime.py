import json

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
def search_anime():
    @task.bash(output_processor=lambda x: json.loads(x))
    def search_for_anime(**context):
        anime = context["params"]["anime"]

        return f"fastanime grab -t '{anime}' --search-results-only"

    @task
    def parse_results(**context):
        ti = context["ti"]

        task_xcom = ti.xcom_pull(task_ids="search_for_anime", key="return_value")
        results = task_xcom["results"]

        if len(results) > 1:
            raise AirflowFailException("More than 1 result returned")

        ti.xcom_push(key="anime_title", value=results[0]["title"])
        ti.xcom_push(key="episode_count", value=results[0]["availableEpisodes"]["sub"])

    search_for_anime() >> parse_results()


search_anime()
