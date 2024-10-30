from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import json


@dag(schedule=None, start_date=days_ago(1), catchup=False)
def search_anime():
    # @task.bash(output_processor=lambda x: json.loads(x))
    # def search_for_anime():
    #     return "fastanime search -t 'cross ange' --search-results-only"

    @task
    def search_for_anime():
        from click.testing import CliRunner
        from fastanime.cli import run_cli

        runner = CliRunner(env={"FASTANIME_CACHE_REQUESTS": "false"})
        result = runner.invoke(run_cli, ["grab", "-t", "'cross ange'", "--search-results-only"])

        print(result)

    search_for_anime()

search_anime()
