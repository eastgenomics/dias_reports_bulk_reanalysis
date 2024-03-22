"""
Script to search through 002 projects in a given date range to find
previously run reports workflows and rerun dias_batch for these samples
with latest / specified assay config file
"""
import argparse
from collections import Counter, defaultdict
import concurrent
from datetime import datetime
from pathlib import Path
import re
from time import sleep
from typing import List, Union
import os

import dxpy


def get_projects(assay, start) -> List[dict]:
    """
    Find 002 projects for given assay from given start date

    Returns
    -------
    list
        list of dicts with details for each project
    """

    projects = sorted(
        dxpy.bindings.search.find_projects(
            name=f"002_*{assay}",
            name_mode="glob",
            created_after=f"-{start}d",
            describe=True,
        ),
        key=lambda x: x["describe"]["name"],
    )

    print(f"Found {len(projects)} projects in past {start} days")
    print(f"Earliest project found: {projects[0]['describe']['name']}")
    print(f"Latest project found: {projects[-1]['describe']['name']}")

    return projects


def get_single_dir(project) -> str:
    """
    Find the Dias single output directory in the project

    Parameters
    ----------
    project : str
        ID of project to check

    Returns
    -------
    str
        Dias single output path
    """
    # project we know have more than one single output directory and
    # have manually selected one
    # TODO - add this to config or something
    single_dirs = {
        "project-GgXvB984QX3xF6qkPK4Kp5xx": "/output/CEN-240304_1257"
    }

    if single_dirs.get(project):
        path = f"{project}:{single_dirs.get(project)}"

        print(f"Using specified Dias single path: {path}")

        return path

    files = list(
        dxpy.find_data_objects(
            project=project,
            name="*multiqc.html",
            name_mode="glob",
            describe=True,
        )
    )

    if len(files) > 1:
        # TODO handle which to choose, should just be one so far
        print(f'More than single path found, multiqc files found')
        for x in files:
            print(x)
        return

    path = f"{project}:{Path(files[0]['describe']['folder']).parent}"

    print(f"Found Dias single path: {path}")

    return path


def get_cnv_call_job(project) -> str:
    """
    Find CNV calling job in project

    Parameters
    ----------
    project : str
        project name to search

    Returns
    -------
    str
        job ID of CNV calling job
    """
    # some projects have multiple (expected) CNV call jobs, manually
    # select the one we want to use output from
    # TODO - move this to a config
    selected_jobs = {
        "project-GgZyg8j47Ky5z0vBG0JBB0QJ":	"job-Ggggppj47Ky46K2KZYyB7J3B",
        "project-GgJ3gf04F80JY20Gjkp0QjF4":	"job-GgPYb984F80JZv63zG198VvZ",
        "project-GZk71GQ446x5YQkjzvpYFBzB":	"job-GZq727Q446x28FQ74BkqBJx9",
        "project-GZ3zJBj4X0Vy0b4Y20QyG1B2":	"job-GZ4q5VQ4X0Vz3jkP95Yb058J",
        "project-GXZg37j4kgGxFZ29fj3f3Yp4":	"job-GXby1ZQ4kgGXQK7gyv506Xj9",
        "project-GXZg0J04BXfPFFZYYFGz42bP":	"job-GXbyZ104BXf8G5296g93bvx2"
    }

    if selected_jobs.get(project):
        job = selected_jobs.get(project)

        print(f"Using specified CNV job: {job}")

        return job

    jobs = list(
        dxpy.find_jobs(
            project=project, name="*GATK*", name_mode="glob", state="done"
        )
    )

    if len(jobs) > 1:
        # TODO handle multiple job IDs returned and None
        print('more than one cnv job found')
        for x in jobs:
            print(x)
        return

    job_id = jobs[0]["id"]
    print(f"CNV job found: {job_id}")

    return job_id


def get_job_states(job_ids) -> dict:
    """
    Query the state of given set of job/analysis IDs

    Parameters
    ----------
    job_ids : list
        list of job IDs to query

    Returns
    -------
    dict
        mapping of job ID to it's state
    """
    job_state = {}

    job_details = call_in_parallel(dxpy.describe, job_ids)

    job_state = {
        job['id']: job['state'] for job in job_details
    }

    return job_state


def get_report_jobs(project) -> List[dict]:
    """
    Get the generate variant workbook jobs

    Parameters
    ----------
    project : str
        project ID to search

    Returns
    -------
    list
        list of jobs details
    """
    jobs = list(
        dxpy.find_jobs(
            project=project,
            name="eggd_generate_variant_workbook",
            state="done",
            describe=True,
        )
    )

    print(f"Found {len(jobs)} generate variant workbook jobs")

    return jobs


def get_launched_workflow_ids(batch_ids) -> list:
    """
    Get analysis IDs of launched Dias reports

    Parameters
    ----------
    batch_ids : list
        list of dias batch job IDs to get launched reports workflows from

    Returns
    -------
    list
        list of reports analysis IDs
    """
    details = call_in_parallel(func=dxpy.describe, items=batch_ids)

    # get the string of comma separated report IDs from every batch job
    # and flatten to a single list
    report_jobs = [x['output']['launched_jobs'] for x in details]
    report_jobs = [jobs.split(',') for jobs in report_jobs]
    report_jobs = [job for jobs in report_jobs for job in jobs]

    return report_jobs


def get_sample_name_and_test_code(job_details) -> Union[str, str]:
    """
    Get the sample name from the xlsx report output from the reports
    job, if the file has been deleted then return None. Parse the test
    code from the clinical indication input to the report job.

    Parameters
    ----------
    job_details : dict
        report job details

    Returns
    -------
    str
        first 2 parts of sample name

    """
    try:
        report_details = dxpy.describe(
            job_details["describe"]["output"]["xlsx_report"]["$dnanexus_link"]
        )
    except dxpy.exceptions.ResourceNotFound:
        # file has been deleted, skip this sample -> clinical indication
        return None, None

    sample = re.match(r"^[\w]+-[\w]+", report_details["name"]).group(0)
    indication = job_details["describe"]["runInput"]["clinical_indication"]

    # parse out R codes and HGNC IDs from clinical indication string
    codes = ','.join(re.findall(r"^[RC][\d]+\.[\d]+|_HGNC:[\d]+", indication))

    if bool(sample) ^ bool(codes):
        print(job_details["describe"]["output"]["xlsx_report"]["$dnanexus_link"])
        print(job_details["describe"]["runInput"])


    return sample, codes


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


def upload_manifest(manifest, path) -> str:
    """
    Upload manifest file to DNAnexus

    Parameters
    ----------
    manifest : str
        filename of manifest to upload
    now : str
        current datetime for folder naming

    Returns
    -------
    str
        file ID of uploaded manifest
    """
    remote_file = dxpy.upload_local_file(
        manifest, folder=path, wait_on_close=True
    )

    # clean up our generated file
    os.remove(manifest)

    return remote_file.id


def call_in_parallel(func, items) -> list:
    """
    Calls the given function in parallel using concurrent.futures on
    the given set of items (i.e for calling dxpy.describe() on multiple
    object IDs)

    Parameters
    ----------
    func : callable
        function to call on each item
    items : list
        list of items to call function on

    Returns
    -------
    list
        list of responses
    """
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        concurrent_jobs = {
            executor.submit(func, item) for item in items
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                results.append(future.result())
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {concurrent_jobs[future]}: {exc}"
                )

    return results


def create_folder(path) -> None:
    """
    Create folder for storing manifests

    Parameters
    ----------
    path : str
        folder to create
    """
    dxpy.bindings.dxproject.DXProject().new_folder(folder=path, parents=True)


def date_to_datetime(date) -> int:
    """
    Turn date str from cmd line to integer of days ago from today

    Parameters
    ----------
    date : str
        date to calculate from

    Returns
    -------
    int
        n days ago from today
    """
    assert len(date) == 6 and re.match(
        r"^2[3|4|5]", date
    ), "Date provided does not seem valid"

    # split parts of date out, removing leading 0 (required for datetime)
    year, month, day = [
        int(date[i : i + 2].lstrip("0")) for i in range(0, len(date), 2)
    ]

    start = datetime(year=int(f"20{year}"), month=month, day=day)

    return (datetime.now() - start).days


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
        mode == 'dias reports workflows'

    print(f"\nMonitoring state of launched {mode}...\n")

    while job_ids:
        job_states = get_job_states(job_ids)
        printable_states = (
            ' | '.join([
                f"{x[0]}: {x[1]}" for x in Counter(job_states.values()).items()
            ])
        )

        # separate failed and done to stop monitoring
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
