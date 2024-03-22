"""
Script to search through 002 projects in a given date range to find
previously run reports workflows and rerun dias_batch for these samples
with latest / specified assay config file
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from time import sleep
from typing import List

import dxpy

from utils.dx_manage import (
    create_folder,
    get_cnv_call_job,
    get_job_states,
    get_launched_workflow_ids,
    get_projects,
    get_report_jobs,
    get_sample_name_and_test_code,
    get_single_dir,
    upload_manifest
)


def generate_manifest(report_jobs, project_name, now) -> List[dict]:
    """
    Generate data to build a manifest by querying the report jobs to get
    the sample name from the output xlsx report and the test code from
    the clinical indication input. This is then written to a file for
    uploading to DNAnexus as input to dias_batch.

    Parameters
    ----------
    report_jobs : list
        list od dicts of describe details for each xlsx report
    project_name : str
        name of project for naming manifest
    now : str
        current datetime for naming

    Returns
    -------
    str
        file name of manifest generated
    """
    print(f"Generating manifest data from {len(report_jobs)} report jobs")
    samples_indications = defaultdict(list)

    sample_codes = call_in_parallel(
        func=get_sample_name_and_test_code,
        items=report_jobs
    )

    for sample, code in sample_codes:
        if sample and code:
            samples_indications[sample].append(code)

    # ensure we don't duplicate test codes for a sample
    samples_indications = {
        sample: list(set(codes))
        for sample, codes in samples_indications.items()
    }

    if samples_indications:
        # found at least one sample report job to rerun
        manifest = f"{project_name}-{now}_re_run.manifest"

        count = 0

        with open(manifest, "w") as fh:
            fh.write(
                "batch\nInstrument ID;Specimen ID;Re-analysis Instrument ID;"
                "Re-analysis Specimen ID;Test Codes\n"
            )

            for sample, codes in samples_indications.items():
                for code in codes:
                    fh.write(f"{sample.replace('-', ';')};;;{code}\n")
                    count += 1

    print(f"{count} sample - test codes written to manifest")

    return manifest


def run_all_batch_jobs(args) -> list:
    """
    Main function to configure all inputs for running dias batch against
    every 002 project in the specified date range

    Parameters
    ----------
    args : argparse.Namespace
        parsed arguments from command line

    Returns
    -------
    list
        list of launched job IDs
    """

    days = date_to_datetime(args.date)

    projects = get_projects(assay=args.assay, start=days)[5:6]

    now = datetime.now().strftime("%y%m%d_%H%M")

    create_folder(path=f"/manifests/{now}")

    launched_jobs = []

    for idx, project in enumerate(projects, 1):
        print(
            f"\nSearching {project['describe']['name']} for jobs "
            f"({idx}/{len(projects)})"
        )

        single_path = get_single_dir(project=project["id"])
        cnv_job = get_cnv_call_job(project=project["id"])
        report_jobs = get_report_jobs(project=project["id"])

        if not single_path and not cnv_job:
            print('single path and / or cnv job not valid, skipping')
            continue

        if not report_jobs:
            print(
                f"No report jobs found in {project}, project will be "
                "ignored as there is nothing to rerun"
            )
            continue

        manifest = generate_manifest(
            report_jobs=report_jobs,
            project_name=project["describe"]["name"],
            now=now,
        )
        manifest_id = upload_manifest(
            manifest=manifest, path=f"/manifests/{now}"
        )

        # name for naming dias batch job
        name = f"eggd_dias_batch_{project['describe']['name']}"

        if args.testing:
            # when testing run everything in one 003 project
            batch_project = "project-Ggvgj6j45jXv43B84Vfzvgv6"
        else:
            batch_project = project["id"]

        batch_id = run_batch(
            project=batch_project,
            cnv_job=cnv_job,
            single_path=single_path,
            manifest=manifest_id,
            name=name,
            assay=args.assay,
            terminate=args.terminate,
        )

        launched_jobs.append(batch_id)

        with open(f"launched_batch_jobs_{now}.log", "a") as fh:
            fh.write(f"{batch_id}\n")

    print(f"Launched {len(launched_jobs)} Dias batch jobs")

    return launched_jobs


def run_batch(
    project, cnv_job, single_path, manifest, name, assay, terminate
) -> str:
    """
    Runs dias batch in the specified project

    Parameters
    ----------
    project : str
        project ID of where to run dias batch
    cnv_job : str | None
        job ID of CNV calling to pass to batch
    single_path : str
        path to Dias single output
    manifest : str
        file ID of uploaded manifest
    name : str
        name to use for batch job
    assay : str
        assay running analysis for
    terminate : bool
        controls if to terminate jobs launched by dias batch (for testing)

    Returns
    -------
    str
        job ID of launched dias batch job
    """
    # only run CNV reports if we have a CNV job ID
    cnv_reports = True if cnv_job else False

    app_input = {
        "cnv_call_job_id": cnv_job,
        "cnv_reports": cnv_reports,
        "snv_reports": True,
        "artemis": True,
        "manifest_files": [{"$dnanexus_link": manifest}],
        "single_output_dir": single_path,
        "assay": assay,
        "testing": terminate,
    }

    job = dxpy.DXApp("app-GfG4Bf84QQg40v7Y6zKF34KP").run(
        app_input=app_input, project=project, name=name
    )

    print(f"Launched dias batch job {job.id} in {project}")

    return job.id


def monitor_launched_jobs(job_ids, mode) -> None:
    """
    Monitor launched Dias batch jobs or reports workflows to ensure all
    complete and alert of any fails to investigate

    Parameters
    ----------
    job_ids : list
        list of job IDs
    mode : str
        string of batch or reports for prettier printing
    """
    failed_jobs = []
    completed_jobs = []
    terminated_jobs = []

    if mode == 'batch':
        mode = 'dias batch jobs'
    else:
        mode = 'dias reports workflows'

    print(f"\nMonitoring state of launched {mode}...\n")

    while job_ids:
        job_states = get_job_states(job_ids)
        printable_states = (
            ' | '.join([
                f"{x[0]}: {x[1]}" for x in Counter(job_states.values()).items()
            ])
        )

        # split failed, terminated (when testing) and done to stop monitoring
        failed = [
            k for k, v in job_states.items()
            if v in ['failed', 'partially failed']
        ]
        done = [k for k, v in job_states.items() if v == 'done']
        terminated = [k for k, v in job_states.items() if v == 'terminated']

        failed_jobs.extend(failed)
        terminated_jobs.extend(terminated)
        completed_jobs.extend(done)

        job_ids = list(set(job_ids) - set(failed))
        job_ids = list(set(job_ids) - set(done))
        job_ids = list(set(job_ids) - set(terminated))

        if not job_ids:
            break

        print(
            f"Waiting on {len(job_ids)} {mode} to "
            f"complete ({printable_states})"
        )
        sleep(30)

    print(
        f"Stopping monitoring launched jobs:\n\t{len(completed_jobs)} "
        f"completed\n\t{len(failed_jobs)} failed ({', '.join(failed_jobs)})"
    )


def parse_args() -> argparse.Namespace:
    """
    Parse cmd line arguments

    Returns
    -------
    argparse.Namespace
        parsed arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--assay", type=str, choices=["CEN", "TWE"], required=True
    )
    parser.add_argument(
        "-d",
        "--date",
        default="230614",
        help=(
            "Earliest date to search for 002 projects, should be in the form "
            "YYMMDD"
        ),
    )
    parser.add_argument(
        "--config", type=str, help="file ID of assay config file to use"
    )
    parser.add_argument(
        "--testing",
        type=bool,
        default=True,
        help=(
            "Controls where dias batch is run, when testing launch all in "
            "one 003 project"
        ),
    )
    parser.add_argument(
        "--terminate",
        type=bool,
        default=True,
        help="Controls if to terminate all jobs dias batch launches",
    )
    parser.add_argument(
        '--monitor', type=bool,
        default=True,
        help=(
            'Controls if to monitor and report on state of launched '
            'dias batch jobs'
        )
    )

    return parser.parse_args()


def main():
    args = parse_args()

    batch_job_ids = run_all_batch_jobs(args=args)

    if args.monitor and batch_job_ids:
        monitor_launched_jobs(batch_job_ids, mode='batch')

        # monitor the launched reports workflows
        report_ids = get_launched_workflow_ids(batch_job_ids)
        monitor_launched_jobs(report_ids, mode='reports')


if __name__ == "__main__":
    main()
