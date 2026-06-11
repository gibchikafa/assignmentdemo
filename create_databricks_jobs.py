"""Create the three Databricks jobs for this assignment repository."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    GitProvider,
    GitSource,
    JobEnvironment,
    PerformanceTarget,
    Source,
    SparkPythonTask,
    Task,
)


DEFAULT_GIT_URL = "https://github.com/gibchikafa/assignmentdemo.git"
DEFAULT_GIT_BRANCH = "main"
DEFAULT_JOB_PREFIX = "assignmentdemo"
DEFAULT_ENVIRONMENT_VERSION = "5"
DEFAULT_DEPENDENCIES = [
    "dlt==1.27.0",
    "pycountry==26.2.16",
    "pandas",
    "jsonschema",
    "python-dateutil",
    "openpyxl",
]


@dataclass(frozen=True)
class JobSpec:
    suffix: str
    task_key: str
    python_file: str
    environment_key: str


JOB_SPECS = [
    JobSpec(
        suffix="basic-ingestion",
        task_key="basic_ingestion",
        python_file="entrypoint.py",
        environment_key="basic_ingestion_environment",
    ),
    JobSpec(
        suffix="incremental-ingestion",
        task_key="incremental_ingestion",
        python_file="entrypoint_incremental.py",
        environment_key="incremental_ingestion_environment",
    ),
    JobSpec(
        suffix="summaries",
        task_key="summaries",
        python_file="entrypoint_summaries.py",
        environment_key="summaries_environment",
    ),
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create the three Databricks jobs for basic, incremental, and summary runs."
    )
    parser.add_argument("--git-url", default=DEFAULT_GIT_URL)
    parser.add_argument("--git-branch", default=DEFAULT_GIT_BRANCH)
    parser.add_argument("--job-prefix", default=DEFAULT_JOB_PREFIX)
    parser.add_argument(
        "--environment-version", default=DEFAULT_ENVIRONMENT_VERSION
    )
    return parser


def job_name(job_prefix: str, suffix: str) -> str:
    return f"{job_prefix}-{suffix}"


def build_environment_spec(environment_key: str, environment_version: str) -> JobEnvironment:
    return JobEnvironment(
        environment_key=environment_key,
        spec=Environment(
            dependencies=list(DEFAULT_DEPENDENCIES),
            environment_version=environment_version,
        ),
    )


def create_job(
    workspace: WorkspaceClient,
    *,
    git_url: str,
    git_branch: str,
    job_prefix: str,
    environment_version: str,
    spec: JobSpec,
):
    return workspace.jobs.create(
        name=job_name(job_prefix, spec.suffix),
        tasks=[
            Task(
                task_key=spec.task_key,
                spark_python_task=SparkPythonTask(
                    python_file=spec.python_file,
                    source=Source.GIT,
                ),
                environment_key=spec.environment_key,
            )
        ],
        git_source=GitSource(
            git_url=git_url,
            git_provider=GitProvider.GIT_HUB,
            git_branch=git_branch,
        ),
        environments=[
            build_environment_spec(spec.environment_key, environment_version)
        ],
        performance_target=PerformanceTarget.PERFORMANCE_OPTIMIZED,
    )


def create_all_jobs(args) -> list[tuple[str, int | str | None]]:
    workspace = WorkspaceClient()
    created = []

    for spec in JOB_SPECS:
        job = create_job(
            workspace,
            git_url=args.git_url,
            git_branch=args.git_branch,
            job_prefix=args.job_prefix,
            environment_version=args.environment_version,
            spec=spec,
        )
        created.append((job_name(args.job_prefix, spec.suffix), getattr(job, "job_id", None)))

    return created


def main() -> None:
    args = build_parser().parse_args([])
    created = create_all_jobs(args)

    print("Created Databricks jobs:")
    for name, job_id in created:
        print(f"- {name}: {job_id}")


if __name__ == "__main__":
    main()

