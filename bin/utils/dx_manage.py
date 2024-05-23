"""
Functions relating to managing data objects an queries in DNAnexus
"""
import concurrent
import os
from pathlib import Path
import re
from typing import List, Union

import dxpy

from .utils import call_in_parallel


def check_archival_state(project, sample_data) -> Union[list, list, list]:
    """
    Check the archival state of all files in a project for the given
    samples that will be required for running reports

    Parameters
    ----------
    project : list
        list of project IDs to use as search scope
    sample_data : list
        list of dicts of per sample details to get samplename from

    Returns
    -------
    list
        list of file objects in live state
    list
        list of file objects in unarchiving state
    list
        list of file objects in archived state
    """
    print("Checking archival state of required files")

    # patterns of sample files required for SNV reports, CNV reports
    # and Artemis
    sample_file_patterns = [
        '_segments.vcf$',
        '_copy_ratios.gcnv.bed$',
        '_copy_ratios.gcnv.bed.tbi$',
        '_markdup.per-base.bed.gz$',
        '_markdup_recalibrated_Haplotyper.vcf.gz$',
        '_markdup.reference_build.txt$',
        'bam$',
        'bam.bai$'
    ]

    # build regex patterns of all files for all samples in blocks of 100
    samples = list(set([x['sample'] for x in sample_data['samples']]))
    files = [f"{x}.*{y}" for x in samples for y in sample_file_patterns]
    files.append(".*_excluded_intervals.bed")

    print(f"{len(samples)} samples to search for")

    files = [
        files[i:i + 100] for i in range(0, len(files), 100)
    ]

    file_details = []

    # TODO - refactor this mess along with find_xlsx_reports(), might be
    # able to bodge it into the call_in_parallel function
    def _find(project, search_term):
        """Query given sample IDs in one go to find all files"""
        return list(dxpy.find_data_objects(
            project=project,
            name=rf'{"|".join(search_term)}',
            name_mode='regexp',
            describe={'fields': {'name': True, 'archivalState': True, 'createdBy': True}}
        ))

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        concurrent_jobs = {
            executor.submit(_find, project, item)
            for item in files
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                output = future.result()
                file_details.append(output)

            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {future}: {exc}"
                )
                raise exc

    # flatten the returned list of lists of sample data
    file_details = [x for y in file_details for x in y]


    # TODO - return something useful from this on states
    print(f"Found {len(file_details)} files")

    live = [x for x in file_details if x['describe']['archivalState'] == 'live']
    unarchiving = [x for x in file_details if x['describe']['archivalState'] == 'unarchiving']
    archived = [x for x in file_details if x['describe']['archivalState'] == 'archived']

    print(
        f"Archival state(s): live {len(live)} | archived {len(archived)} | "
        f"unarchiving {len(unarchiving)}"
    )

    return live, unarchiving, archived


def create_folder(path) -> None:
    """
    Create folder for storing manifests

    Parameters
    ----------
    path : str
        folder to create
    """
    dxpy.bindings.dxproject.DXProject().new_folder(folder=path, parents=True)


def get_cnv_call_job(project, selected_jobs) -> list:
    """
    Find CNV calling job in project

    Parameters
    ----------
    project : str
        project name to search
    selected_jobs : dict
        mapping of project ID to job ID of projects where multiple CNV
        call jobs have been run and we have manually selected one

    Returns
    -------
    str
        list of CNV calling job IDs
    """
    if selected_jobs.get(project):
        job = selected_jobs.get(project)

        print(
            f"Using previously selected CNV job from project where multiple "
            f"CNV calling jobs run {job}"
        )

        return [job]

    jobs = list(
        dxpy.find_jobs(
            project=project, name="*GATK*", name_mode="glob", state="done"
        )
    )

    jobs = [x['id'] for x in jobs]

    return jobs


def get_dependent_files(project_samples):
    """
    TODO

    Parameters
    ----------
    project_samples : _type_
        _description_
    """
    for project, samples in project_samples.values():
        pass


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

    # ensure we don't check failed batch jobs
    details = [x for x in details if x['state'] == 'done']

    # get the string of comma separated report IDs from every batch job
    # and flatten to a single list
    report_jobs = [
        x.get("output", {}).get("launched_jobs") for x in details if x
    ]
    report_jobs = [jobs.split(",") for jobs in report_jobs]
    report_jobs = [job for jobs in report_jobs for job in jobs]

    return report_jobs


def get_projects(assay) -> List[dict]:
    """
    Find 002 projects for given assay from given start date

    Parameters
    ----------
    assay : str
        assay to search for

    Returns
    -------
    list
        list of dicts with details for each project
    """

    projects = sorted(
        dxpy.bindings.search.find_projects(
            name=f"002_*{assay}",
            name_mode="glob",
            describe=True,
        ),
        key=lambda x: x["describe"]["name"],
    )

    print(f"Found {len(projects)} projects for {assay}")

    # turn list of projects to dict of id: describe
    projects = {x['id']: x['describe'] for x in list(reversed(projects))}

    return projects


def get_xlsx_reports(all_samples, projects) -> list:
    """
    Return all xlsx report objects for the given samples across all the
    given projects

    Parameters
    ----------
    samples : list
        list of part of samplename to search for xlsx file for
    projects : list
        list of project IDs to search within

    Returns
    -------
    list
        list of all xlsx file describe objects found
    """
    def get_reports(project, samples):
        """Query given sample IDs in one go to find all files"""
        return list(dxpy.find_data_objects(
            project=project,
            name=rf'.*({"|".join(samples)}).*xlsx',
            name_mode='regexp',
            describe={'fields': {'name': True, 'archivalState': True, 'createdBy': True}}
        ))

    print(f"Searching {len(projects)} projects for samples")

    # create chunks of 100 samples from our sample list for querying
    chunked_samples = [
        all_samples[i:i + 100] for i in range(0, len(all_samples), 100)
    ]

    all_reports = []

    for project in projects:
        project_reports = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            concurrent_jobs = {
                executor.submit(get_reports, project, item)
                for item in chunked_samples
            }

            for future in concurrent.futures.as_completed(concurrent_jobs):
                # access returned output as each is returned in any order
                try:
                    reports = future.result()
                    project_reports.append(reports)

                except Exception as exc:
                    # catch any errors that might get raised during querying
                    print(
                        f"Error getting data for {future}: {exc}"
                    )
                    raise exc

        # flatten the returned list of lists of sample data
        project_reports = [x for y in project_reports for x in y]
        all_reports.extend(project_reports)
        print(f"Found {len(project_reports)} reports in project {project}")

    # filter out any xlsx files found that look to also have a run ID
    # in the name => output from eggd_artemis for a single sample
    # (example run ID: 240229_A01295_0328_BHYG25DRX3)
    all_reports = [
        x for x in all_reports if not re.search(
            r'[\d]+_[A-Z][\d]+_[\d]{4}_[\w][^_]+', x['describe']['name']
        )
    ]

    print(f"Found {len(all_reports)} total xlsx reports for all projects")

    return all_reports


def get_single_dir(project, selected_paths) -> str:
    """
    Find the Dias single output directory in the project

    Parameters
    ----------
    project : str
        ID of project to check
    selected_paths : dict
        mapping of project ID to path where multiple Dias single attempts
        have been run and we have manually selected one

    Returns
    -------
    str
        Dias single output path
    """
    if selected_paths.get(project):
        path = f"{project}:{selected_paths.get(project)}"

        print(
            f"Using manually specified Dias single path where more than one "
            f"exists in the project: {path}"
        )

        return [path]

    # find Dias single output using multiQC report as a proxy
    files = list(
        dxpy.find_data_objects(
            project=project,
            name="*multiqc.html",
            name_mode="glob",
            describe=True,
        )
    )

    paths = [
        f"{project}:{Path(x['describe']['folder']).parent}" for x in files
    ]

    print(f"Found Dias single path(s): {paths}")

    return paths


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
