"""
Script to search through 002 projects in a given date range to find
previously run reports workflows and rerun dias_batch for these samples
with latest / specified assay config file
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from time import sleep
from typing import List

import dxpy

from utils.dx_manage import (
    check_archival_state,
    create_folder,
    get_cnv_call_job,
    get_job_states,
    get_launched_workflow_ids,
    get_projects,
    get_xlsx_reports,
    get_single_dir,
    upload_manifest,
)


from utils.utils import (
    call_in_parallel,
    filter_non_unique_specimen_ids,
    group_samples_by_project,
    parse_clarity_export,
    parse_sample_identifiers
)


def configure_inputs(samples, assay):
    """
    Searches all 002 projects against given sample list to find
    original project for each and

    Parameters
    ----------
    samples : dict
        mapping of specimen ID to list of test codes
    """
    projects = list(reversed(get_projects(assay=assay)))

    manual_review = defaultdict(lambda: defaultdict(list))
    print(list(samples.keys()))
    reports = get_xlsx_reports(
        all_samples=list(samples.keys()),
        projects=projects
    )

    samples = parse_sample_identifiers(reports)
    samples, non_unique_specimens = filter_non_unique_specimen_ids(samples)

    project_samples = group_samples_by_project(samples=samples)

    print(
        f"{len(project_samples.keys())} projects retained with samples"
        " to run reports for"
    )

    projects_to_skip = []

    for project, project_data in project_samples.items():
        cnv_jobs = get_cnv_call_job(project=project)
        dias_single_paths = get_single_dir(project=project)

        if len(cnv_jobs) > 1:
            print('oh no - more than one cnv job found')
            projects_to_skip.append(project)
            #TODO - figure out what to do
            continue
        else:
            project_samples[project]['cnv_call_job'] = cnv_jobs[0]

        if len(dias_single_paths) > 1:
            print('oh no - more than one single path found')
            projects_to_skip.append(project)
            continue
        else:
            project_samples[project]['dias_single_path'] = dias_single_paths[0]

        check_archival_state(project=project, sample_data=project_data)

    # remove any projects worth of samples with issues
    for project in projects_to_skip:
        project_samples.pop(project)


    exit()


def generate_manifest(report_jobs, project_name, now) -> List[dict]:
    """
    Generate data to build a manifest by querying the report jobs to get
    the sample name from the output xlsx report and the test code from
    the clinical indication input. This is then written to a file for
    uploading to DNAnexus as input to dias_batch.

    Parameters
    ----------
    report_jobs : list
        list of dicts of describe details for each xlsx report
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
        func=get_sample_name_and_test_code, items=report_jobs
    )

    for sample, code in sample_codes:
        if sample and code:
            samples_indications[sample].append(code)

    # ensure we don't duplicate test codes for a sample
    samples_indications = {
        sample: list(set(codes))
        for sample, codes in samples_indications.items()
    }

    manifest = None

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
    every 002 project

    Parameters
    ----------
    args : argparse.Namespace
        parsed arguments from command line

    Returns
    -------
    list
        list of launched job IDs
    """
    projects = get_projects(assay=args.assay)

    if args.limit:
        print(f"Limiting rerunning jobs to {args.limit} runs")
        projects = projects[: args.limit]

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
            print("single path and / or cnv job not valid, skipping")
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

        if not manifest:
            # didn't generate any data to make a manifest file
            print("No valid previous job data found to generate manifest file")
            continue

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
            batch_inputs=args.batch_inputs,
            assay=args.assay,
            terminate=args.terminate,
        )

        launched_jobs.append(batch_id)

        with open(f"launched_batch_jobs_{now}.log", "a") as fh:
            fh.write(f"{batch_id}\n")

    print(f"Launched {len(launched_jobs)} Dias batch jobs")

    return launched_jobs


def run_batch(
    project,
    cnv_job,
    single_path,
    manifest,
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
    cnv_job : str | None
        job ID of CNV calling to pass to batch
    single_path : str
        path to Dias single output
    manifest : str
        file ID of uploaded manifest
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
        "single_output_dir": single_path,
        "assay": assay,
        "testing": terminate,
    }

    if batch_inputs:
        app_input = {**app_input, **batch_inputs}

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
        "--clarity_export", type=str,help=(
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
        required=False,
        type=int,
        help="number of runs to limit running jobs for",
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
        samples_to_codes = parse_clarity_export(args.clarity_export)

    configure_inputs(samples_to_codes, 'CEN')





    batch_job_ids = run_all_batch_jobs(args=args)

    if args.monitor and batch_job_ids:
        monitor_launched_jobs(batch_job_ids, mode="batch")

        # monitor the launched reports workflows
        report_ids = get_launched_workflow_ids(batch_job_ids)
        monitor_launched_jobs(report_ids, mode="reports")


if __name__ == "__main__":
    main()
