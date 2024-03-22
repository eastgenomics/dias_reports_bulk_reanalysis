"""
Script to search through 002 projects in a given date range to find
previously run reports workflows and rerun dias_batch for these samples
with latest / specified assay config file
"""
import argparse
from collections import defaultdict
import concurrent
from datetime import datetime
from pathlib import Path
import re
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
            name=f'002_*{assay}',
            name_mode='glob',
            created_after=f"-{start}d",
            describe=True
        ),
        key = lambda x: x['describe']['name']
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
    files = list(dxpy.find_data_objects(
        project=project,
        name="*multiqc.html",
        name_mode="glob",
        describe=True
    ))

    if len(files) > 1:
        # TODO handle which to choose, should just be one so far
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
    jobs = list(dxpy.find_jobs(
        project=project,
        name="*GATK*",
        name_mode="glob",
        state="done"
    ))

    if len(jobs) > 1:
        # TODO handle multiple job IDs returned and None
        return

    job_id = jobs[0]['id']
    print(f"CNV job found: {job_id}")

    return job_id


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
    jobs = list(dxpy.find_jobs(
        project=project,
        name="eggd_generate_variant_workbook",
        state="done",
        describe=True
    ))

    print(f"Found {len(jobs)} generate variant workbook jobs\n")

    return jobs


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
    Union[str, None]
        first 2 parts of sample name if file still exists, None if
        the file has been deleted
    """
    try:
        report_details = dxpy.describe(
            job_details['describe']['output']['xlsx_report']['$dnanexus_link']
        )
    except dxpy.exceptions.ResourceNotFound:
        # file has been deleted, skip this sample -> clinical indication
        return None, None

    sample = re.match(r'^[\w]+-[\w]+', report_details['name']).group(0)
    code = job_details['describe']['runInput']['clinical_indication'].split('_')[0]

    return sample, code


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

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        concurrent_jobs = {
            executor.submit(
                get_sample_name_and_test_code, job
            ) for job in report_jobs
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                sample, code = future.result()

                if sample and code:
                    samples_indications[sample].append(code)
                else:
                    print('foo')
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(f"Error getting data for {concurrent_jobs[future]}: {exc}")


    # ensure we don't duplicate test codes for a sample
    samples_indications = {
        sample: list(set(codes)) for sample, codes in samples_indications.items()
    }

    if samples_indications:
        # found at least one sample report job to rerun
        manifest = f"{project_name}-{now}_re_run.manifest"

        count  = 0

        with open(manifest, 'w') as fh:
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
        manifest,
        folder=path,
        wait_on_close=True
    )

    # clean up our generated file
    os.remove(manifest)

    return remote_file.id


def run_batch(
    project,
    cnv_job,
    single_path,
    manifest,
    name,
    assay,
    terminate
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
        'cnv_call_job_id': cnv_job,
        'cnv_reports': cnv_reports,
        'snv_reports': True,
        'artemis': True,
        'manifest_files': [{
            "$dnanexus_link": manifest
        }],
        'single_output_dir': single_path,
        'assay': assay,
        'testing': terminate
    }

    job = dxpy.DXApp('app-GfG4Bf84QQg40v7Y6zKF34KP').run(
        app_input=app_input,
        project=project,
        name=name
    )

    print(f"Launched dias batch job {job.id} in {project}")

    return job.id


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
    assert len(date) == 6 and re.match(r'^2[3|4|5]', date), (
        'Date provided does not seem valid'
    )

    # split parts of date out, removing leading 0 (required for datetime)
    year, month, day = [
        int(date[i:i+2].lstrip('0')) for i in range(0, len(date), 2)
    ]

    start = datetime(year=int(f"20{year}"), month=month, day=day)

    return (datetime.now() - start).days


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
        '-a', '--assay', type=str, choices=['CEN', 'TWE']
    )
    parser.add_argument(
        '-d', '--date', default='230614',
        help=(
            'Earliest date to search for 002 projects, should be in the form '
            'YYMMDD'
        )
    )
    parser.add_argument(
        '--config', type=str, help='file ID of assay config file to use'
    )
    parser.add_argument(
        '--testing', type=bool, default=True,
        help=(
            'Controls where dias batch is run, when testing launch all in '
            'one 003 project'
        )
    )
    parser.add_argument(
        '--terminate', type=bool, default=True,
        help="Controls if to terminate all jobs dias batch launches"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    days = date_to_datetime(args.date)

    projects = get_projects(assay=args.assay, start=days)

    now = datetime.now().strftime('%y%m%d_%H%M')

    create_folder(path=f"/manifests/{now}")

    for project in projects[5:]:
        print(f"\nSearching {project['describe']['name']}...")

        single_path = get_single_dir(project=project['id'])
        cnv_job = get_cnv_call_job(project=project['id'])
        report_jobs = get_report_jobs(project=project['id'])

        if not report_jobs:
            print(
                f"No report jobs found in {project}, project will be "
                "ignored as there is nothing to rerun"
            )
            continue

        manifest = generate_manifest(
            report_jobs=report_jobs,
            project_name=project['describe']['name'],
            now=now
        )
        manifest_id = upload_manifest(
            manifest=manifest,
            path=f"/manifests/{now}"
            )

        # name for naming dias batch job
        name = f"eggd_dias_batch_{project['describe']['name']}"

        if args.testing:
            # when testing run everything in one 003 project
            batch_project = 'project-Ggvgj6j45jXv43B84Vfzvgv6'
        else:
            batch_project = project['id']

        batch_job = run_batch(
            project=batch_project,
            cnv_job=cnv_job,
            single_path=single_path,
            manifest=manifest_id,
            name=name,
            assay=args.assay,
            terminate=args.terminate
        )

        break

if __name__ == "__main__":
    main()
