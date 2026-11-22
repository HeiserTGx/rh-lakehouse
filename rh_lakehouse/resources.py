import json
import time
from datetime import timedelta

import dagster as dg
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service import jobs, sql

FINISHED = {
    sql.StatementState.SUCCEEDED,
    sql.StatementState.FAILED,
    sql.StatementState.CANCELED,
    sql.StatementState.CLOSED,
}


class DatabricksResource(dg.ConfigurableResource):
    """The Databricks workspace that does the compute. Dagster holds no data and
    runs no Spark: it submits each pipeline step as a serverless run and reads
    back the counts that step reports."""

    host: str
    token: str
    warehouse_id: str
    target_schema: str
    git_branch: str
    git_url: str = "https://github.com/HeiserTGx/rh-lakehouse"

    def client(self) -> WorkspaceClient:
        return WorkspaceClient(host=self.host, token=self.token)

    def run_step(self, step: str, **parameters: int) -> tuple[dict, str]:
        """Run one step of pipeline/run_step on serverless compute, with the code
        taken from this repo on GitHub. Returns the step's counts and the URL of
        the run in Databricks."""
        client = self.client()
        waiter = client.jobs.submit(
            run_name=f"rh-lakehouse {step}",
            git_source=jobs.GitSource(
                git_url=self.git_url,
                git_provider=jobs.GitProvider.GIT_HUB,
                git_branch=self.git_branch,
            ),
            tasks=[
                jobs.SubmitTask(
                    task_key=step,
                    notebook_task=jobs.NotebookTask(
                        notebook_path="pipeline/run_step",
                        source=jobs.Source.GIT,
                        base_parameters={
                            "step": step,
                            "schema": self.target_schema,
                            **{name: str(value) for name, value in parameters.items()},
                        },
                    ),
                )
            ],
        )
        try:
            run = waiter.result(timeout=timedelta(minutes=30))
        except OperationFailed as failure:
            # The run's own state only says that it failed. The error raised
            # inside the notebook is on the task's output.
            task = client.jobs.get_run(waiter.run_id).tasks[0]
            error = client.jobs.get_run_output(task.run_id).error
            raise dg.Failure(f"Databricks step {step!r} failed: {error}") from failure

        output = client.jobs.get_run_output(run.tasks[0].run_id)
        return json.loads(output.notebook_output.result), run.run_page_url

    def query(self, statement: str) -> list[list[str]]:
        """Run one SQL statement on the SQL warehouse and return its rows."""
        statements = self.client().statement_execution
        response = statements.execute_statement(
            warehouse_id=self.warehouse_id, statement=statement, wait_timeout="30s"
        )
        # A stopped warehouse has to start first, which can outlast the wait above.
        while response.status.state not in FINISHED:
            time.sleep(5)
            response = statements.get_statement(response.statement_id)
        if response.status.state != sql.StatementState.SUCCEEDED:
            raise dg.Failure(f"Query failed: {response.status.error.message}")
        return response.result.data_array
