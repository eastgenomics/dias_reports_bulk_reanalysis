"""
Functions relating to managing data objects an queries in DNAnexus
"""
import os
from pathlib import Path
import re
from typing import List, Union

import dxpy

from .utils import call_in_parallel


def create_folder(path) -> None:
    """
    Create folder for storing manifests

    Parameters
    ----------
    path : str
        folder to create
    """
    dxpy.bindings.dxproject.DXProject().new_folder(folder=path, parents=True)


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
        "project-GgZyg8j47Ky5z0vBG0JBB0QJ": "job-Ggggppj47Ky46K2KZYyB7J3B",
        "project-GgJ3gf04F80JY20Gjkp0QjF4": "job-GgPYb984F80JZv63zG198VvZ",
        "project-GZk71GQ446x5YQkjzvpYFBzB": "job-GZq727Q446x28FQ74BkqBJx9",
        "project-GZ3zJBj4X0Vy0b4Y20QyG1B2": "job-GZ4q5VQ4X0Vz3jkP95Yb058J",
        "project-GXZg37j4kgGxFZ29fj3f3Yp4": "job-GXby1ZQ4kgGXQK7gyv506Xj9",
        "project-GXZg0J04BXfPFFZYYFGz42bP": "job-GXbyZ104BXf8G5296g93bvx2",
    }

    if selected_jobs.get(project):
        job = selected_jobs.get(project)

        print(
            f"Using previously selected CNV job from project where multiple "
            f"CNV calling jobs run {job}"
        )

        return job

    jobs = list(
        dxpy.find_jobs(
            project=project, name="*GATK*", name_mode="glob", state="done"
        )
    )

    if len(jobs) > 1:
        # TODO handle multiple job IDs returned and None
        print("more than one cnv job found")
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

    job_state = {job["id"]: job["state"] for job in job_details}

    return job_state


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
    report_jobs = [x["output"]["launched_jobs"] for x in details]
    report_jobs = [jobs.split(",") for jobs in report_jobs]
    report_jobs = [job for jobs in report_jobs for job in jobs]

    return report_jobs


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
    codes = ",".join(re.findall(r"^[RC][\d]+\.[\d]+|_HGNC:[\d]+", indication))

    if bool(sample) ^ bool(codes):
        # TODO - remember what this was meant to be for
        print(
            job_details["describe"]["output"]["xlsx_report"]["$dnanexus_link"]
        )
        print(job_details["describe"]["runInput"])

    return sample, codes


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
        "project-GgXvB984QX3xF6qkPK4Kp5xx": "/output/CEN-240304_1257",
        "project-Ggyb2G84zJ4363x2JqfGgb6J ": "/output/CEN-240322_0936"
    }

    if single_dirs.get(project):
        path = f"{project}:{single_dirs.get(project)}"

        print(
            f"Using manually specified Dias single path where more than one "
            f"exists in the project: {path}"
        )

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
        print("More than one single output path found from multiqc reports")
        for x in files:
            print(x)
        return

    path = f"{project}:{Path(files[0]['describe']['folder']).parent}"

    print(f"Found Dias single path: {path}")

    return path


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

    # clean up our generated local file
    os.remove(manifest)

    return remote_file.id
