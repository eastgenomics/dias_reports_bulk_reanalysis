"""
Functions relating to managing data objects an queries in DNAnexus
"""
import concurrent
from datetime import datetime
import os
from pathlib import Path
import re
from typing import List, Union

import dxpy
import pandas as pd
from tqdm import tqdm

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
        '_copy_ratios.gcnv.bed.gz$',
        '_copy_ratios.gcnv.bed.gz.tbi$',
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

    print(f"{len(files)} sample files to search for")

    file_details = find_in_parallel(
        project=project,
        items=files
    )

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


def check_job_state(jobs) -> dict:
    """
    Checks the job state of all jobs

    Assumed valid execution states include:
        idle, waiting_on_input, runnable, running, waiting_on_output, done,
        restarted, restartable, debug_hold, failed, terminating, terminated

    Parameters
    ----------
    job_ids : list
        list of dx job details

    Returns
    -------
    dict
        dict of in_progress, failed and done job lists
    """
    print(f"\nChecking state of {len(jobs)} jobs\n")

    in_progress_states = [
        'idle', 'waiting_on_input', 'runnable', 'running', 'waiting_on_output',
        'restarted', 'restartable'
    ]

    failed_states = ['debug_hold', 'failed', 'terminating', 'terminated']

    all_job_states = {}

    all_job_states['in_progress'] = [
        job for job in jobs if job['state'] in in_progress_states
    ]
    all_job_states['failed'] = [
        job for job in jobs if job['state'] in failed_states
    ]
    all_job_states['done'] = [
        job for job in jobs if job['state'] == 'done'
    ]

    print(
        f"Total job states:\n\tin progress: {len(all_job_states['in_progress'])}\n\t"
        f"done: {len(all_job_states['done'])}\n\tfailed: "
        f"{len(all_job_states['failed'])}"
    )

    if all_job_states['failed']:
        failed_job_projects = sorted(set([
            x['project'] for x in all_job_states['failed']
        ]))

        base_url = (
            "https://platform.dnanexus.com/panx/projects/"
            "PROJECT/monitor?state.values=failed%2Cterminating"
            "%2Cterminated%2CpartiallyFailed"
        )

        failed_urls = '\n\t'.join([
            base_url.replace('PROJECT', x) for x in failed_job_projects
        ])

        print(
            f"\nWARNING: {len(all_job_states['failed'])} jobs in a failed / "
            f"terminated state.\n\nProjects with failed jobs:\n\t{failed_urls}"
        )

    return all_job_states


def unarchive_files(project_files) -> None:
    """
    Unarchive given file IDs that are dependent for running reports.

    Adapted from eggd_dias_batch.dx_manage.unarchive_files():
    https://github.com/eastgenomics/eggd_dias_batch/blob/b63a04e2d421a246017e984efcc2a9eef85fbeaf/resources/home/dnanexus/dias_batch/utils/dx_requests.py#L472

    Parameters
    ----------
    project_files : dict
        mapping of project ID -> list archived file objects

    Raises
    ------
    RuntimeError
        Raised if unarchiving fails for a set of project files
    """
    print(
        f"\nUnarchiving {len([x for y in project_files.values() for x in y])} "
        f"files in {len(project_files.keys())} project(s)..."
    )

    for project, files in project_files.items():
        try:
            dxpy.api.project_unarchive(
                project,
                input_params={
                    "files": [x['id'] for x in files]
                }
            )
        except Exception as error:
            # API spec doesn't list the potential exceptions raised,
            # catch everything and exit on any error
            print(
                "Error unarchving files for "
                f"{files[0]['project']}: {error}"
            )
            raise RuntimeError(f"Error unarchiving files: {error}")


    # build a handy command to dump into the stdout for people to check
    # the state of all of the files we're unarchiving later on
    check_state_cmd = (
        f"echo {' '.join([x['id'] for y in project_files.values() for x in y])}"
        " | xargs -n1 -d' ' -P32 -I{} bash -c 'dx describe --json {} ' | "
        "grep archival | uniq -c"
    )

    print(
        f"\n \nUnarchiving requested for {len(files)} files, this "
        "will take some time...\n \n"
    )

    print(
        "The state of all files may be checked with the following command:"
        f"\n \n\t{check_state_cmd}\n \n"
    )

    exit()


def download_single_file(dxid, project, path) -> None:
    """
    Given a single dx file ID, downloads it with the original filename

    Parameters
    ----------
    dxid : str
        file ID of object to download
    project : str
        project containing the file
    path : str
        path to download file to
    """
    dxpy.bindings.dxfile_functions.download_dxfile(
        dxid,
        os.path.join(path, dxpy.describe(dxid).get('name')),
        project=project
    )


def create_folder(project, path) -> None:
    """
    Create folder for storing manifests

    Parameters
    ----------
    project : str
        project to create folder in
    path : str
        folder to create
    """
    dxpy.bindings.dxproject.DXProject(dxid=project).new_folder(
        folder=path, parents=True
    )


def find_in_parallel(project, items, prefix='', suffix='') -> list:
    """
    Call dxpy.find_data_objects in parallel for given list of `items`.

    All items in list are chunked into max 100 items and queried in one
    go as a regex pattern for more efficient querying

    Parameters
    ----------
    project : str
        project ID in which to restrict search scope
    items : list
        list of search terms to search for
    prefix : str
        optional prefix string for searching
    suffix : str
        optional suffix string for searching

    Returns
    -------
    list
        list of all found dxpy object details
    """
    def _find(project, search_terms):
        """Query given patterns as a regex search term to find all files"""
        return list(dxpy.find_data_objects(
            project=project,
            name="|".join([f"{prefix}{x}{suffix}" for x in search_terms]),
            name_mode='regexp',
            describe={
                'fields': {
                    'name': True,
                    'archivalState': True,
                    'createdBy': True
                }
            }
        ))

    results = []

    # create chunks of 100 items from list for querying
    chunked_items = [items[i:i + 100] for i in range(0, len(items), 100)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        concurrent_jobs = {
            executor.submit(_find, project, item) for item in chunked_items
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                results.extend(future.result())

            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {future}: {exc}"
                )
                raise exc

    return results


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


def get_launched_workflow_ids(batch_ids) -> Union[list, list]:
    """
    Get analysis IDs of launched Dias reports and job ID of eggd_artemis

    Parameters
    ----------
    batch_ids : list
        list of dias batch job IDs to get launched reports workflows from

    Returns
    -------
    list
        list of eggd_artemis jobs, n.b. this should always be a single
        job but keeping this as a list to handle future changes
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

    # the only single jobs *should* be eggd_artemis here since we aren't
    # running CNV calling, split this from the reports jobs
    artemis_jobs = [x for x in report_jobs if x.startswith('job-')]
    report_jobs = [x for x in report_jobs if x.startswith('analysis-')]

    return artemis_jobs, report_jobs


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
    print(f"Searching {len(projects)} projects for samples")

    all_reports = []

    for project in tqdm(projects, ncols=100):
        project_reports = []

        project_reports = find_in_parallel(
            project=project,
            items=all_samples,
            prefix='.*',
            suffix='.*xlsx'
        )
        all_reports.extend(project_reports)

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


def get_single_dir(project, selected_paths) -> list:
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
    list
        list of Dias single output path(s)
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


def get_multiqc_report(single_path) -> list:
    """
    Finds the multiQC report in the given Dias single path

    Parameters
    ----------
    snv_path : str
        path to snv reports

    Returns
    -------
    list
        file ID(s) of the multiqc file(s)
    """
    project, path = single_path.split(':')

    reports = list(dxpy.find_data_objects(
        project=project,
        folder=path,
        name="*-multiqc.html",
        name_mode='glob',
        describe=True
    ))

    return [x['id'] for x in reports]


def get_latest_dias_batch_app() -> str:
    """
    Get the app ID of the latest eggd_dias_batch

    Returns
    -------
    str
        app ID of latest version of eggd_dias_batch

    Raises
    ------
    AssertionError
        Raised if no matching app found
    """
    app = list(dxpy.bindings.search.find_apps(
        name='eggd_dias_batch',
        name_mode='exact',
        all_versions=False,
        published=True
    ))

    assert app, "No app found for eggd_dias_batch"

    return app[0]['id']


def run_batch(
    project,
    batch_app_id,
    cnv_job,
    single_path,
    manifest,
    multiqc_report_id,
    name,
    batch_inputs,
    assay,
    terminate,
    ) -> str:
    """
    Runs dias batch in the specified project

    Parameters
    ----------
    project : str
        project ID of where to run dias batch
    batch_app_id : str
        app ID of latest version of eggd_dias_batch
    cnv_job : str | None
        job ID of CNV calling to pass to batch
    single_path : str
        path to Dias single output
    manifest : str
        file ID of uploaded manifest
    multiqc_report_id : str
        file ID of multiQC report to pass to eggd_artemis
    name : str
        name to use for batch job
    batch_inputs : dict
        dict of additional inputs to provide to dias_batch
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
        "multiqc_report": {"$dnanexus_link": multiqc_report_id},
        "single_output_dir": single_path,
        "assay": assay,
        "testing": terminate,
    }

    if batch_inputs:
        app_input = {**app_input, **batch_inputs}

    job = dxpy.DXApp(batch_app_id).run(
        app_input=app_input, project=project, name=name
    )

    return job.id


def get_latest_genepanels_file() -> dict:
    """
    Find the latest genepanels file from 001_Reference project in DNAnexus.

    Returns
    -------
    dict
        file describe details of latest genepanels file

    Raises
    ------
    RuntimeError
        Raised if no genepanels found in DNAnexus 001_Reference project
    """
    files = list(dxpy.find_data_objects(
        project="project-Fkb6Gkj433GVVvj73J7x8KbV",
        folder="/dynamic_files/gene_panels/",
        name="*tsv",
        name_mode="glob",
        describe={'fields': {
            'created': True,
            'name': True
        }}
    ))

    if not files:
        raise RuntimeError(
            "No genepanels files found in project-Fkb6Gkj433GVVvj73J7x8KbV"
            "/dynamic_files/gene_panels/"
        )

    # get latest file by created key
    latest_file = sorted(
        files,
        reverse=True,
        key=lambda x: datetime.fromtimestamp(x['describe']['created']/1000)
    )[0]

    print(f"Latest genepanels file selected: {latest_file['describe']['name']}")

    return latest_file


def read_genepanels_file(file_details) -> pd.DataFrame:
    """
    Read genepanels file into DataFrame.

    Adapted from eggd_dias_batch.utils.parse_genepanels:
    https://github.com/eastgenomics/eggd_dias_batch/blob/master/resources/home/dnanexus/dias_batch/utils/utils.py#L311

    This will keep the unique rows from the first 2 columns (i.e. one
    row per clinical indication / panel), and adds the test code as a
    separate column.

    Example resultant dataframe:

    +-----------+-----------------------+---------------------------+
    | test_code |      indication       |        panel_name         |
    +-----------+-----------------------+---------------------------+
    | C1.1      | C1.1_Inherited Stroke |  CUH_Inherited Stroke_1.0 |
    | C2.1      | C2.1_INSR             |  CUH_INSR_1.0             |
    +-----------+-----------------------+---------------------------+

    Parameters
    ----------
    file_details : dict
        file describe details of latest genepanels file

    Returns
    -------
    pd.DataFrame
        DataFrame of genepanels file
    """
    contents = dxpy.DXFile(
        project=file_details['project'],
        dxid=file_details['id']
    ).read().rstrip('\n').split('\n')

    # genepanels file may have 3 or 4 columns as it can also contain HGNC
    # ID and PanelApp panel ID, just use the first 2 columns
    genepanels = pd.DataFrame(
        [x.split('\t')[:2] for x in contents],
        columns=['indication', 'panel_name']
    )
    genepanels.drop_duplicates(keep='first', inplace=True)
    genepanels.reset_index(inplace=True, drop=True)

    return genepanels


def upload_manifest(manifest, project, path) -> str:
    """
    Upload manifest file to DNAnexus

    Parameters
    ----------
    manifest : str
        filename of manifest to upload
    project : str
        DNAnexus project to upload file to
    path : str
        remote path to upload file to

    Returns
    -------
    str
        file ID of uploaded manifest
    """
    remote_file = dxpy.upload_local_file(
        manifest,
        project=project,
        folder=path,
        wait_on_close=True
    )

    # clean up our generated local file
    os.remove(manifest)

    return remote_file.get_id()
