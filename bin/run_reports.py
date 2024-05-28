"""
Script to search through 002 projects in a given date range to find
previously run reports workflows and rerun dias_batch for these samples
with latest / specified assay config file
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from os import path
from time import sleep
from typing import List


from utils.dx_manage import (
    check_archival_state,
    create_folder,
    get_cnv_call_job,
    get_job_states,
    get_launched_workflow_ids,
    get_projects,
    get_xlsx_reports,
    get_single_dir,
    get_latest_dias_batch_app,
    run_batch,
    upload_manifest,
)


from utils.utils import (
    add_clarity_data_back_to_samples,
    filter_non_unique_specimen_ids,
    filter_clarity_samples_with_no_reports,
    group_samples_by_project,
    limit_samples,
    parse_config,
    parse_clarity_export,
    parse_sample_identifiers
)


TEST_PROJECT = "project-Ggvgj6j45jXv43B84Vfzvgv6"


def configure_inputs(clarity_data, assay, limit, start_date, end_date):
    """
    Searches all 002 projects against given sample list to find
    original project for each, check the archivalState for all
    required files and return big dict of all data.

    Returned data structure will be:

    {
        "project-xxx": {
            "project_name": "002_240401_A01295_0334_XXYNSHDBDR",
            "cnv_call_job_id": "job-xxx",
            "dias_single_path": "project-xxx:/output/CEN_240401_1105",
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

    Returns
    -------
    dict
        mapping of all samples and their respective data per project
    """
    manual_cnv_call_jobs, manual_dias_single_paths = parse_config()

    projects = get_projects(assay=assay)

    manual_review = defaultdict(lambda: defaultdict(list))

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

    project_samples = group_samples_by_project(
        samples=samples,
        projects=projects
    )

    projects_to_skip = []

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

        if len(cnv_jobs) > 1:
            print('oh no - more than one cnv job found')
            projects_to_skip.append(project_id)
            #TODO - figure out what to do, should we stop on any with issues?
            continue
        else:
            # add in CNV call job ID for current project
            project_samples[project_id]['cnv_call_job_id'] = cnv_jobs[0]

        if len(dias_single_paths) > 1:
            print('oh no - more than one single path found')
            projects_to_skip.append(project_id)
            continue
        else:
            # add in Dias single path for current project
            project_samples[project_id]['dias_single_path'] = dias_single_paths[0]

        _, unarchiving, archived = check_archival_state(
            project=project_id, sample_data=project_data
        )

        if unarchiving or archived:
            # TODO -  figure out what to do with this projects worth of samples
            # where there are non-live files we require
            print('Archived or unarchiving files present')
            projects_to_skip.append(project_id)
            continue

    # remove any projects worth of samples with issues
    # TODO - figure out what / how to handle these for reviewing and resuming,
    # can probably pickle a load of data and resume from there
    for project in projects_to_skip:
        project_samples.pop(project)

    return project_samples


def write_manifest(project_name, sample_data, now) -> List[dict]:
    """
    Write Epic manifest file of all samples for given project

    Parameters
    ----------
    project_name : str
        name of project for naming manifest
    sample_data : list
        list of dicts of sample data (IDs and test code(s))
    now : str
        current datetime for naming

    Returns
    -------
    str
        file name of manifest generated
    """
    print(f"Generating manifest data for {len(sample_data)} samples")

    manifest = f"{project_name}-{now}_re_run.manifest"
    count = 0

    with open(manifest, "w") as fh:
        fh.write(
            "batch\nInstrument ID;Specimen ID;Re-analysis Instrument ID;"
            "Re-analysis Specimen ID;Test Codes\n"
        )

        for sample in sample_data:
            for code in sample['codes']:
                fh.write(
                    f"{sample['instrument_id']};{sample['specimen_id']}"
                    f";;;{code}\n"
                )
                count += 1

    print(f"{count} sample - test codes written to file {manifest}")

    return manifest


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

    create_folder(path=f"/manifests/{now}")
    batch_app_id = get_latest_dias_batch_app()

    launched_jobs = []

    for project, project_data in all_sample_data.items():

        manifest = write_manifest(
            sample_data=project_data['samples'],
            project_name=project_data['project_name'],
            now=now
        )

        manifest_id = upload_manifest(
            manifest=manifest, path=f"/manifests/{now}"
        )

        # name for naming dias batch job
        name = f"eggd_dias_batch_{project_data['project_name']}"

        if args.testing:
            # when testing run everything in one 003 project
            batch_project = TEST_PROJECT
        else:
            batch_project = project["id"]

        batch_id = run_batch(
            project=batch_project,
            batch_app_id=batch_app_id,
            cnv_job=project_data['cnv_call_job_id'],
            single_path=project_data['dias_single_path'],
            manifest=manifest_id,
            name=name,
            batch_inputs=args.batch_inputs,
            assay=args.assay,
            terminate=args.terminate
        )

        launched_jobs.append(batch_id)

        job_id_log = path.join(
            path.dirname(path.abspath(__file__)),
            f"../../logs/launched_batch_jobs_{now}.log"
        )

        with open(job_id_log, "a") as fh:
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
    parser.add_argument(
        "-a", "--assay", type=str, choices=["CEN", "TWE"], required=True
    )
    clarity = parser.add_mutually_exclusive_group(required=True)
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
    parser.add_argument(
        "--config",
        type=str,
        help=(
            "file ID of assay config file to use, if not specified "
            "will select latest from 001_Reference"
        ),
    )
    parser.add_argument(
        "--batch_inputs",
        type=str,
        help=(
            "JSON formatted string of additional inputs to pass to dias_batch "
            "e.g. '{\"unarchive\": True}'"
        ),
    )
    parser.add_argument(
        "--limit",
        default=None,
        type=int,
        help=(
            "number of samples to limit running jobs for, if no date range is "
            "specified this will default to being the oldest n samples"
        )
    )
    parser.add_argument(
        "--start_date",
        default=None,
        type=str,
        help=(
            "Earliest date to select samples from Clarity to run reports for, "
            "to be specified as YYMMDD"
        )
    )
    parser.add_argument(
        "--end_date",
        default=None,
        type=str,
        help=(
            "Latest date to select samples from Clarity to run reports for, "
            "to be specified as YYMMDD"
        )
    )
    parser.add_argument(
        "--testing",
        action='store_true',
        default=False,
        help=(
            "Controls where dias batch is run, when testing launch all in "
            "one 003 project"
        ),
    )
    parser.add_argument(
        "--terminate",
        action='store_true',
        default=False,
        help="Controls if to terminate all analysis jobs dias batch launches",
    )
    parser.add_argument(
        "--monitor",
        type=bool,
        default=True,
        help=(
            "Controls if to monitor and report on state of launched "
            "dias batch jobs"
        ),
    )

    args = parser.parse_args()

    if args.batch_inputs:
        args = verify_batch_inputs_argument(args)

    input_str = '\n\t'.join(f"{k} : {v}" for k, v in args.__dict__.items())
    print(f"Specified arguments:\n\t{input_str}\n")

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


def main():
    args = parse_args()

    if args.clarity_connect:
        pass
    else:
        clarity_data = parse_clarity_export(args.clarity_export)

    sample_data = configure_inputs(
        clarity_data=clarity_data,
        assay=args.assay,
        limit=args.limit,
        start_date=args.start_date,
        end_date=args.end_date
    )

    print(
        f"\nConfirm running reports for all samples in "
        f"{TEST_PROJECT if args.testing else 'original 002 projects'}"
    )
    while True:
        confirm = input('Run jobs? ')

        if confirm.lower() in ['y', 'yes']:
            print("Beginning launching jobs...")
            break
        elif confirm.lower() in ['n', 'no']:
            print("Stopping now.")
            exit()
        else:
            print("Invalid response, please enter 'y' or 'n'")

    batch_job_ids = run_all_batch_jobs(args=args, all_sample_data=sample_data)

    if args.monitor and batch_job_ids:
        monitor_launched_jobs(batch_job_ids, mode="batch")

        # monitor the launched reports workflows
        report_ids = get_launched_workflow_ids(batch_job_ids)
        monitor_launched_jobs(report_ids, mode="reports")


if __name__ == "__main__":

    main()
