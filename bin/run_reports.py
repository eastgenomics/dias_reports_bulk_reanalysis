"""
Script to search through 002 projects in a given date range to find
previously run reports workflows and rerun dias_batch for these samples
with latest / specified assay config file
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from os import makedirs, path, remove
from time import sleep
from typing import List

import dxpy

from utils.dx_manage import (
    check_archival_state,
    check_job_state,
    unarchive_files,
    create_folder,
    download_single_file,
    get_cnv_call_job,
    get_job_states,
    get_launched_workflow_ids,
    get_projects,
    get_xlsx_reports,
    get_single_dir,
    get_multiqc_report,
    get_latest_dias_batch_app,
    run_batch,
    upload_manifest,
    get_latest_genepanels_file,
    read_genepanels_file
)


from utils.utils import (
    add_clarity_data_back_to_samples,
    call_in_parallel,
    filter_non_unique_specimen_ids,
    filter_clarity_samples_with_no_reports,
    group_samples_by_project,
    group_dx_objects_by_project,
    filter_reports_with_variants,
    limit_samples,
    parse_config,
    parse_clarity_export,
    parse_sample_identifiers,
    validate_test_codes,
    write_manifest,
    write_to_log,
    read_from_log
)


def configure_inputs(clarity_data, assay, limit, start_date, end_date, unarchive):
    """
    Searches all 002 projects against given sample list to find
    original project for each, check the archivalState for all
    required files and return big dict of all data.

    Returned data structure will be:

    {
        "project-xxx": {
            "project_name": "002_240401_A01295_0334_XXYNSHDBDR",
            "cnv_call_job_id": "job-xxx",
            "dias_single": "project-xxx:/output/CEN_240401_1105",
            "samples": [
                {
                    "sample": "123456-23251R0047",
                    "instrument_id": "123456",
                    "specimen_id": "23251R0047",
                    "codes": ["R211"]
                },
                ...
            ]
        }
    }

    Parameters
    ----------
    clarity_data : dict
        mapping of specimen ID to list of test codes and dates from Clarity
    assay : str
        assay to run reports for
    limit : int
        no. of samples to limit for rerunning
    start_date : int
        earliest date of samples in Clarity to restrict running reports for
    end_date : int
        latest date of samples in Clarity to restrict running reports for
    unarchive : bool
        controls if to unarchive any archived files

    Returns
    -------
    dict
        mapping of all samples and their respective data per project
    """
    manual_cnv_call_jobs, manual_dias_single_paths = parse_config()

    projects = get_projects(assay=assay)

    reports = get_xlsx_reports(
        all_samples=list(clarity_data.keys()),
        projects=list(projects.keys())
    )

    samples = parse_sample_identifiers(reports)
    samples, non_unique_specimens = filter_non_unique_specimen_ids(samples)

    filter_clarity_samples_with_no_reports(
        clarity_samples=clarity_data,
        samples_w_reports=samples
    )

    # add back the test codes and booked date from Clarity for each sample
    samples = add_clarity_data_back_to_samples(
        samples=samples,
        clarity_data=clarity_data
    )

    if any([limit, start_date, end_date]):
        samples = limit_samples(
            samples=samples,
            limit=limit,
            start=start_date,
            end=end_date
        )

    # check all test codes from Clarity valid against latest genepanels
    latest_genepanels = get_latest_genepanels_file()
    genepanels = read_genepanels_file(file_details=latest_genepanels)
    samples, invalid_sample_tests = validate_test_codes(
        all_sample_data=samples,
        genepanels=genepanels
    )

    if invalid_sample_tests:
        # write log of the samples with invalid tests
        invalid_test_log = (
            f"{datetime.today().strftime('%y%m%d_%H%M')}"
            "_invalid_test_codes.json"
        )

        invalid_test_log = path.abspath(path.join(
            path.dirname(path.abspath(__file__)),
            f"../logs/{invalid_test_log}"
        ))

        with open(invalid_test_log, 'w') as fh:
            json.dump(invalid_sample_tests, fh)

    project_samples = group_samples_by_project(
        samples=samples,
        projects=projects
    )

    # dict we will build up of project ID -> issues, will include:
    # - unhandled instances of multiple CNV call jobs or Dias single
    # - unarchiving / archived files
    # - missing / multiple multiQC reports in given single dir
    manual_review = defaultdict(dict)

    for project_id, project_data in project_samples.items():
        print(
            f"\nChecking project data for {project_data['project_name']} "
            f"({project_id})"
        )
        cnv_jobs = get_cnv_call_job(
            project=project_id,
            selected_jobs=manual_cnv_call_jobs
        )
        dias_single_paths = get_single_dir(
            project=project_id,
            selected_paths=manual_dias_single_paths
        )

        multiqc_report = get_multiqc_report(
            single_path=dias_single_paths[0]
        )

        if len(cnv_jobs) == 0:
            # no CNV reports, raise error if this is for CEN, print
            # warning if this is WES (for now)
            if assay == 'CEN':
                manual_review[project_id]['cnv_call'] = 'No CNV call job found'
            else:
                print(
                    '\nWARNING: no CNV calling job found for '
                    f'{project_data["project_name"]} ({project_id}), '
                    'continuing with job launching since this is for WES\n'
                )
        elif len(cnv_jobs) > 1:
            # unhandled multiple CNV call job => throw in error bucket
            manual_review[project_id]['cnv_call'] = cnv_jobs
        else:
            # add in CNV call job ID for current project
            project_samples[project_id]['cnv_call_job_id'] = cnv_jobs[0]

        if len(dias_single_paths) > 1:
            # unhandled multiple Dias single output => throw in error bucket
            manual_review[project_id]['dias_single'] = dias_single_paths
        else:
            project_samples[project_id]['dias_single'] = dias_single_paths[0]

        # verify found single multiQC report in single dir
        if len(multiqc_report) == 0:
            manual_review[project_id]['multiqc'] = None
        elif len(multiqc_report) > 1:
            manual_review[project_id]['multiqc'] = multiqc_report
        else:
            project_samples[project_id]['multiqc'] = multiqc_report[0]


        _, unarchiving, archived = check_archival_state(
            project=project_id, sample_data=project_data
        )

        if unarchiving:
            manual_review[project_id]['unarchiving'] = unarchiving

        if archived:
            manual_review[project_id]['archived'] = archived

        if manual_review.get(project_id):
            # project has issues, add project name for nicer printing
            manual_review[project_id][
                'project_name'] = project_data['project_name']

    if manual_review:
        # one or more issues with some samples
        print("\nWarning - one or more projects have issues...")
        for project_id, issues in manual_review.items():
            print(f"\nIssues with {issues['project_name']} ({project_id}):")

            if issues.get('cnv_call'):
                print(
                    "\tProject has not got a single CNV calling job and is "
                    f"not specified in config: {issues.get('cnv_call')}"
                )

            if issues.get('dias_single'):
                print(
                    "\tProject has more than one Dias single output dir and "
                    f"is not specified in config: {issues.get('dias_single')}"
                )

            if issues.get('unarchiving'):
                print(
                    f"\t{len(issues.get('unarchiving'))} files are "
                    "still unarchiving"
                )

            if issues.get('archived'):
                print(
                    f"\t{len(issues.get('archived'))} required files are in "
                    "an archived state"
                )

            if issues.get('multiqc'):
                print(
                    "Did not find single multiQC report in project: "
                    f"{issues.get('multiqc')}"
                )

        if unarchive and [x.get('archived') for x in manual_review.values()]:
            unarchive_files(
                project_files={
                    k: v['archived'] for k, v in manual_review.items()
                }
            )

        print("\nExiting now due to above listed issues.")
        exit()

    return project_samples


def run_all_batch_jobs(args, all_sample_data) -> list:
    """
    Main function to configure all inputs for running dias batch against
    every 002 project

    Parameters
    ----------
    args : argparse.Namespace
        parsed arguments from command line
    all_sample_data : dict
        mapping of project ID to all sample data required

    Returns
    -------
    list
        list of launched job IDs
    """
    now = datetime.now().strftime("%y%m%d_%H%M")

    batch_app_id = get_latest_dias_batch_app()

    launched_jobs = []

    for project, project_data in all_sample_data.items():

        if args.test_project:
            batch_project = args.test_project
        else:
            batch_project = project

        if args.ignore_test_codes:
            codes_project, codes_file = args.ignore_test_codes.split(':')
            dxpy.bindings.dxfile_functions.download_dxfile(
                codes_file,
                "codes.txt",
                project=codes_project
            )
            with open("codes.txt") as f:
                codes_to_strip = f.read().splitlines()
            remove("codes.txt")
        else:
            codes_to_strip = []

        manifest = write_manifest(
            sample_data=project_data['samples'],
            project_name=project_data['project_name'],
            now=now,
            ignore_codes=codes_to_strip
        )

        create_folder(
            project=batch_project,
            path=f"/manifests/{now}"
        )

        manifest_id = upload_manifest(
            manifest=manifest,
            project=batch_project,
            path=f"/manifests/{now}"
        )

        # name for naming dias batch job
        name = f"eggd_dias_batch_{project_data['project_name']}"

        batch_id = run_batch(
            project=batch_project,
            batch_app_id=batch_app_id,
            cnv_job=project_data.get('cnv_call_job_id'),
            single_path=project_data['dias_single'],
            manifest=manifest_id,
            multiqc_report_id=project_data['multiqc'],
            name=name,
            batch_inputs=args.batch_inputs,
            assay=args.assay,
            terminate=args.terminate
        )

        print(
            f"Launched dias batch job in {batch_project} ({batch_id}) "
            f"with manifest {manifest}"
        )

        launched_jobs.append(batch_id)

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

    if mode == "batch":
        mode = "dias batch jobs"
    else:
        mode = "dias reports workflows"

    print(f"\nMonitoring state of launched {mode}...\n")

    while job_ids:
        job_states = get_job_states(job_ids)
        printable_states = " | ".join(
            [f"{x[0]}: {x[1]}" for x in Counter(job_states.values()).items()]
        )

        # split failed, terminated (when testing) and done to stop monitoring
        failed = [
            k
            for k, v in job_states.items()
            if v in ["failed", "partially failed"]
        ]
        done = [k for k, v in job_states.items() if v == "done"]
        terminated = [k for k, v in job_states.items() if v == "terminated"]

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
    subparsers = parser.add_subparsers(
        help='mode to run', dest='mode', required=True
    )

    reanalysis_parser = subparsers.add_parser(
        'reanalysis',
        help='mode to perform reanalysis from Clarity samples'
    )

    reanalysis_parser.add_argument(
        "-a", "--assay", type=str, choices=["CEN", "TWE"], required=True
    )

    clarity = reanalysis_parser.add_mutually_exclusive_group(required=True)
    clarity.add_argument(
        "--clarity_export", type=str, help=(
            'export from Clarity to parse samples from if not connecting'
        )
    )
    clarity.add_argument(
        "--clarity_connect", action="store_true", help=(
            "controls if to connect to Clarity to retrieve samples awaiting "
            "analysis and their respective test codes"
        )
    )
    reanalysis_parser.add_argument(
        "--config",
        type=str,
        help=(
            "file ID of assay config file to use, if not specified "
            "will select latest from 001_Reference"
        ),
    )
    reanalysis_parser.add_argument(
        "--batch_inputs",
        type=str,
        help=(
            "JSON formatted string of additional inputs to pass to dias_batch "
            "e.g. '{\"unarchive\": True}'"
        ),
    )
    reanalysis_parser.add_argument(
        "--limit",
        default=None,
        type=int,
        help=(
            "number of samples to limit running jobs for, if no date range is "
            "specified this will default to being the oldest n samples"
        )
    )
    reanalysis_parser.add_argument(
        "--start_date",
        default=None,
        type=str,
        help=(
            "Earliest date to select samples from Clarity to run reports for, "
            "to be specified as YYMMDD"
        )
    )
    reanalysis_parser.add_argument(
        "--end_date",
        default=None,
        type=str,
        help=(
            "Latest date to select samples from Clarity to run reports for, "
            "to be specified as YYMMDD"
        )
    )
    reanalysis_parser.add_argument(
        "--unarchive",
        default=None,
        action="store_true",
        help="controls if to start unarchiving of any required files"
    )
    reanalysis_parser.add_argument(
        "--test_project",
        type=str,
        help=(
            "DNAnexus project to run all batch jobs and analysis in for "
            "testing, if not specified will launch jobs in original 002 "
            "projects"
        ),
    )
    reanalysis_parser.add_argument(
        "--terminate",
        action='store_true',
        default=False,
        help="Controls if to terminate all analysis jobs dias batch launches",
    )
    reanalysis_parser.add_argument(
        "--monitor",
        action='store_true',
        default=True,
        help=(
            "Controls if to monitor and report on state of launched "
            "dias batch jobs"
        ),
    )
    reanalysis_parser.add_argument(
        "--ignore_test_codes",
        type=str,
        help=(
            "DNAnexus file ID of file containing test codes to ignore and not "
            "add to the manifest. Each line of the file should contain a test "
            "code. Should be in the format project-123456:file-123456"
        ),
    )

    download_parser = subparsers.add_parser(
        'download',
        help='mode to download outputs from a log file of job IDs'
    )

    download_parser.add_argument(
        '--job_log', type=str, required=True, help=(
            'JSON log file output from reanalysis mode from which to download '
            'the outputs of all Dias reports jobs'
        )
    )
    download_parser.add_argument(
        '--path', type=str, required=True, help=(
            'path to directory to download reports to'
        )
    )

    args = parser.parse_args()

    if args.mode == 'reanalysis':
        if args.batch_inputs:
            args = verify_batch_inputs_argument(args)

    input_str = '\n\t'.join(f"{k} : {v}" for k, v in args.__dict__.items())
    print(f"\nSpecified arguments:\n\t{input_str}\n")

    return args


def verify_batch_inputs_argument(args):
    """
    Verifies that the inputs provided to --batch_inputs are a valid JSON
    string and that all are valid inputs to eggd_dias_batch app

    Parameters
    ----------
    args : argparse.Namespace
        parsed arguments

    Returns
    -------
    argparse.Namespace
        parsed arguments

    Raises
    ------
    AssertionError
        Raised if any of the given inputs are not valid dias_batch inputs
    RuntimeError
        Raised if not a valid JSON string
    """
    try:
        args.batch_inputs = json.loads(args.batch_inputs)
    except json.decoder.JSONDecodeError as exc:
        raise RuntimeError(
            "Failed to parse --batch_inputs as JSON string"
        ) from exc

    valid_inputs = {
        "assay_config_dir",
        "cnv_call_job_id",
        "exclude_samples",
        "manifest_subset",
        "qc_file",
        "multiqc_report",
        "assay_config_file",
        "exclude_samples_file",
        "exclude_controls",
        "split_tests",
        "sample_limit" "unarchive",
    }

    invalid_inputs = set(args.batch_inputs.keys()) - valid_inputs

    assert (
        not invalid_inputs
    ), f"Invalid inputs provided to --batch_inputs: {invalid_inputs}"

    return args


def download_all_reports(log_file, output_path) -> None:
    """
    Downloads all output xlsx reports, coverage reports, artemis files
    and multiQC reports from the given log file of dias batch jobs.

    We're going to query the launched jobs of all dias batch jobs instead
    of trusting what was added to the local log file in case of monitoring
    not being specified.

    This will be downloaded to the directory structure of a single
    folder per project the jobs were run in.

    Parameters
    ----------
    log_file : str
        log file to read job IDs from
    output_path : str
        path of where to download files to

    Raises
    ------
    SystemExit
        Will exit with zero exit code on any jobs being still in progress
    """
    job_ids = read_from_log(log_file=log_file)
    batch_job_ids = job_ids.get('dias_batch')

    # get the launched jobs of all logged batch jobs
    batch_details = call_in_parallel(dxpy.describe, batch_job_ids)
    launched_job_ids = [
        x['output'].get('launched_jobs', '').split(',') for x in batch_details
    ]
    launched_job_ids = [x for y in launched_job_ids for x in y]

    print(
        f"{len(launched_job_ids)} jobs from {len(batch_job_ids)} dias batch "
        f"jobs to download output reports from. Checking details of all "
        "launched jobs...\n"
    )

    job_details = call_in_parallel(dxpy.describe, launched_job_ids)

    # check the state of all launched jobs before downloading
    all_job_states = check_job_state(job_details)

    if all_job_states['in_progress']:
        # one or more jobs not complete => don't try download
        print(
            f"WARNING: {len(all_job_states['in_progress'])} jobs are still in "
            f"progress. Will not download any reports until these complete."
        )
        exit()

    project_job_details = group_dx_objects_by_project(job_details)

    count = 0

    for project_id, project_data in project_job_details.items():
        count += 1
        print(
            f"\n[{count}/{len(project_job_details.keys())}] Downloading "
            f"files for {project_data['project_name']} ({project_id})\n"
        )

        snv_report_jobs = [
            x for x in project_data['items'] if x['id'].startswith('analysis-')
            and 'dias_reports' in x['executableName']
        ]
        cnv_report_jobs = [
            x for x in project_data['items'] if x['id'].startswith('analysis-')
            and 'dias_cnvreports' in x['executableName']
        ]
        artemis_jobs = [
            x for x in project_data['items'] if x['name'] == 'eggd_artemis'
        ]

        # get just snv and cnv reports (plus coverage reports) for reports
        # where there are some variants filtered
        snv_ids = filter_reports_with_variants(
            reports=snv_report_jobs,
            report_field='stage-rpt_generate_workbook.xlsx_report'
        )

        cnv_ids = filter_reports_with_variants(
            reports=cnv_report_jobs,
            report_field='stage-cnv_generate_workbook.xlsx_report'
        )

        artemis_links_ids = multiqc_ids = []

        if artemis_jobs:
            artemis_links_ids = [
                x['output']['url_file']['$dnanexus_link'] for x in artemis_jobs
                if x['output']
            ]

            multiqc_ids = [
                x['input']['multiqc_report'] for x in artemis_jobs
                if x['input'].get('multiqc_report', {}).get('$dnanexus_link')
            ]

        if not any([snv_ids, cnv_ids, artemis_links_ids]):
            print(
                f"\nNo reports with variants or eggd_artemis output to "
                f"download for {project_data['project_name']}"
            )
            continue

        # create local run dir for downloading to
        project_path = path.join(output_path, project_data['project_name'])
        makedirs(project_path, exist_ok=True)

        print(
            f"Downloading {int(len(snv_ids) / 2) + len(cnv_ids)} xlsx reports, "
            f"{int(len(snv_ids) / 2)} coverage reports, "
            f"{len(artemis_links_ids)} links files and "
            f"{len(multiqc_ids)} multiQC reports"
        )

        call_in_parallel(
            download_single_file,
            snv_ids + cnv_ids + artemis_links_ids + multiqc_ids,
            ignore_missing=True,
            project=project_id,
            path=project_path
        )

        print(f"\nCompleted downloading files to {project_path}")


def main():
    args = parse_args()

    if args.mode == 'download':
        download_all_reports(
            log_file=args.job_log,
            output_path=args.path
        )
        exit()

    if args.clarity_connect:
        pass
    else:
        clarity_data = parse_clarity_export(args.clarity_export)

    sample_data = configure_inputs(
        clarity_data=clarity_data,
        assay=args.assay,
        limit=args.limit,
        start_date=args.start_date,
        end_date=args.end_date,
        unarchive=args.unarchive
    )

    total_samples = sum([
        len(x.get('samples', [])) for x in sample_data.values()
    ])

    print(
        f"\nConfirm running reports for all {total_samples} samples with "
        f"{len(sample_data.keys())} dias batch jobs in "
        f"{args.test_project if args.test_project else 'original 002 projects'}"
    )
    while True:
        confirm = input('Run jobs? ')

        if confirm.lower() in ['y', 'yes']:
            print("\nBeginning launching jobs...")
            break
        elif confirm.lower() in ['n', 'no']:
            print("Stopping now.")
            exit()
        else:
            print("Invalid response, please enter 'y' or 'n'")

    now = datetime.today().strftime('%y%m%d_%H%M')
    launched_job_log = f"launched_jobs_{now}_log.json"

    batch_job_ids = run_all_batch_jobs(args=args, all_sample_data=sample_data)

    write_to_log(
        log_file=launched_job_log,
        key='dias_batch',
        job_ids=batch_job_ids
    )

    if args.monitor and batch_job_ids:
        monitor_launched_jobs(batch_job_ids, mode="batch")

        # monitor the launched reports workflows
        artemis_ids, report_ids = get_launched_workflow_ids(batch_job_ids)

        write_to_log(
            log_file=launched_job_log,
            key='dias_reports',
            job_ids= report_ids
        )

        write_to_log(
            log_file=launched_job_log,
            key='eggd_artemis',
            job_ids= artemis_ids
        )

        monitor_launched_jobs(report_ids, mode="reports")

if __name__ == "__main__":

    main()
