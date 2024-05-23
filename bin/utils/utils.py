"""
General utility functions
"""
from collections import defaultdict
from copy import deepcopy
import concurrent
from datetime import datetime
import json
from os import path
import re

import pandas as pd
from typing import Union


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
        concurrent_jobs = {executor.submit(func, item) for item in items}

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                results.append(future.result())
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {concurrent_jobs[future]}: {exc}"
                )
                raise exc

    return results


def date_str_to_datetime(date) -> int:
    """
    Turn 6 digit date str of yymmdd into datetime object

    Parameters
    ----------
    date : str|int
        date to convert

    Returns
    -------
    datetime
        datetime object of given str | int
    """
    date = str(date)

    assert len(date) == 6, "Date provided does not seem valid"

    # split parts of date out, removing leading 0 (required for datetime)
    year, month, day = [
        int(date[i : i + 2].lstrip("0")) for i in range(0, len(date), 2)
    ]

    return datetime(year=int(f"20{year}"), month=month, day=day)


def filter_non_unique_specimen_ids(reports) -> Union[list, dict]:
    """
    Filter out any samples that exist in more than one 002 project by
    specimen ID where we can't unambiguously identify them against a
    single instrument ID. This is being done since we only extract the
    specimen ID from Clarity.

    Parameters
    ----------
    reports : list
        list of sample report objects to filter down

    Returns
    -------
    list
        sample reports where specimen ID - instrument ID is unique
    dict
        mapping of specimen ID to report dict where more than one
        instrument ID is present for a specimen ID
    """
    unique = []
    non_unique = defaultdict(list)

    # first map specimen to unique instrument IDs
    sample_map = defaultdict(list)
    for report in reports:
        if report['instrument_id'] not in sample_map[report['specimen_id']]:
            sample_map[report['specimen_id']].append(report['instrument_id'])

    # split out reports where the specimen matches more than one instrument
    for report in reports:
        if len(sample_map[report['specimen_id']]) > 1:
            non_unique[report['specimen_id']].append(report)
        else:
            unique.append(report)

    return unique, non_unique


def filter_clarity_samples_with_no_reports(clarity_samples, samples_w_reports):
    """
    Filter the sample list by the reports returned from DNAnexus to
    highlight which have no reports and won't be run

    Parameters
    ----------
    clarity_samples : dict
        dict of samples specimen IDs to test codes and dates parsed
        from Clarity
    samples_w_reports : list
        list of sample identifiers for those with xlsx reports
    """
    # pre-filter all specimen IDs from reports data
    reports_specimens = [s.get('specimen_id') for s in samples_w_reports]

    clarity_w_reports = {
        specimen: data for specimen, data in clarity_samples.items()
        if specimen in reports_specimens
    }
    clarity_w_out_reports = {
        specimen: data for specimen, data in clarity_samples.items()
        if specimen not in reports_specimens
    }

    print(
        "Total no. of outstanding samples from Clarity with no prior reports "
        f"in DNAnexus: {len(clarity_w_out_reports.keys())}"
    )
    print(
        f"Total samples available to run reports for: "
        f"{len(clarity_w_reports.keys())}"
    )

    # TODO - figure out if we need to do anything return here


def group_samples_by_project(samples, projects) -> dict:
    """
    Group the list of sample reports by the project they are from.

    Returns as the structure:
    {
        'project-xxx': {
            'project_name': '002_240401_A01295_0334_XXYNSHDBDR',
            'samples': [
                {
                    'sample': '123456-23251R0047',
                    'instrument_id': '123456',
                    'specimen_id': '23251R0047'
                }
                ...
            ]
        }
    },
    {
        'project-yyy': {
            ...


    Parameters
    ----------
    samples : list
        list of sample details
    projects : dict
        describe details of all 002 projects

    Returns
    -------
    list
        report object lists split by project
    """
    project_samples = defaultdict(lambda: defaultdict(list))

    for sample in samples:
        project_samples[sample['project']]['samples'].append(sample)
        project_samples[sample['project']]['project_name'] = projects.get(
            sample['project']).get('name')

    return {k: dict(v) for k, v in dict(project_samples).items()}


def add_clarity_data_back_to_samples(sample_codes, project_samples) -> dict:
    """
    Add in the test codes and date as additional keys for each sample
    to the project_samples dict

    Parameters
    ----------
    sample_codes : dict
        mapping of specimen ID to test codes and date from Clarity
    project_samples : dict
        per project sample data to add test codes to

    Returns
    -------
    dict
        per project sample data with test codes and date added
    """
    project_samples_with_codes = deepcopy(project_samples)

    for project_id, project_data in project_samples.items():
        for idx, sample_data in enumerate(project_data['samples']):
            codes = list(set(
                sample_codes.get(sample_data['specimen_id']).get('codes')
            ))
            date = sample_codes.get(sample_data['specimen_id']).get('date')

            if not codes:
                # this shouldn't happen since we've taken the specimen ID
                # from the sample codes dict to make the project_samples dict
                # TODO - do something here useful
                print('oh no')

            project_samples_with_codes[project_id]['samples'][idx]['codes'] = codes
            project_samples_with_codes[project_id]['samples'][idx]['date'] = date

    return project_samples_with_codes


def limit_samples(projects_samples, limit, start, end) -> dict:
    """
    Limits the number of samples retained in the mapping of project ->
    sample lists to run jobs for

    Parameters
    ----------
    projects_samples : dict
        mapping of project ID to sample lists
    limit : int
        number of samples to limit by
    start : int
        earliest date of samples in Clarity to restrict running reports for
    end : int
        latest date of samples in Clarity to restrict running reports for

    Returns
    -------
    dict
        mapping of project ID to sample lists
    """
    # set date defaults if not specified
    if start:
        start = date_str_to_datetime(start)
    else:
        start = datetime(year=1970, month=1, day=1)

    if not end:
        end = datetime.now().strftime('%y%m%d')

    end = date_str_to_datetime(end)

    print(
        "\nLimiting samples retained for running reports, currently have "
        f"{sum([len(x['samples']) for x in projects_samples.values()])} "
        f"samples in {len(projects_samples.keys())} projects\nLimits "
        f"specified:\n\tMaximum number samples: {limit}\n\tDate range: "
        f"{start.strftime('%Y-%m-%d')} : {end.strftime('%Y-%m-%d')}"
    )

    limited_project_samples = defaultdict(lambda x: defaultdict(list))
    sample_dates = []
    selected_samples = 0
    hit_limit = False

    for project, project_data in projects_samples.items():
        if hit_limit: break

        print(project_data)
        exit()

        for sample in project_data['samples']:
            if selected_samples >= limit:
                hit_limit = True
                print(f"Hit limit of {limit} samples to retain")
                break

            if not start <= sample['date'] <= end:
                continue

            # sample within date range and not hit limit => select it
            if not limited_project_samples.get(project):
                limited_project_samples[project] = {
                    'project_name': project_data['project_name'],
                    'samples': []
                }

            limited_project_samples[project]['samples'].append(sample)
            sample_dates.append(sample['date'])
            selected_samples += 1

    print(
        f"{sum([len(x['samples']) for x in limited_project_samples.values()])}"
        f" samples retained in {len(limited_project_samples.keys())} projects "
        f"from {min(sample_dates).strftime('%Y-%m-%d')} to "
        f"{max(sample_dates).strftime('%Y-%m-%d')}"
    )

    exit()

    return limited_project_samples


def parse_config() -> Union[dict, dict]:
    """
    Parse config file of manually specified Dias single paths and CNV
    call job IDs.

    These are stored in configs/manually_selected.json, and are required
    where more than one Dias single path / CNV call job is present for
    a given project and cannot be unambiguously selected.

    Returns
    -------
    dict
        mapping of project IDs -> CNV call job IDs
    dict
        mapping of project IDs -> Dias single paths
    """
    config = path.abspath(path.join(
        path.dirname(path.abspath(__file__)),
        "../../configs/manually_selected.json"
    ))

    with open(config) as fh:
        contents = json.load(fh)

    return contents.get('cnv_call_jobs'), contents.get('dias_single_paths')


def parse_clarity_export(export_file) -> dict:
    """
    Parse the xlsx export from Clarity to get the samples and test codes

    Parameters
    ----------
    export_file : str
        file name of Clarity export to parse

    Returns
    -------
    dict
        dict mapping specimen ID to test code(s) and booked date
    """
    clarity_df = pd.read_excel(export_file)

    clarity_df['Specimen Identifier'] = clarity_df[
        'Specimen Identifier'].str.replace('SP-', '')

    # remove any cancelled and pending samples
    clarity_df = clarity_df[clarity_df['Test Validation Status'] == 'Resulted']

    clarity_df = clarity_df[
        clarity_df['Test Directory Clinical Indication'] != 'Research Use'
    ]

    clarity_df['Test Directory Clinical Indication'].fillna(
        value='', inplace=True
    )

    # turn the date time column into just valid date type
    clarity_df['Received Specimen Date Time'] = pd.to_datetime(
        clarity_df['Received Specimen Date Time']
    ).dt.strftime('%y%m%d')

    # TODO - need to figure out if to use the Clinical Indication column
    # to get the test code from or the Test Code column, they seem to differ
    # so need to know which is correct

    # generate mapping of specimen ID to list of test codes and booked date
    sample_code_mapping = {
        x['Specimen Identifier']: {
            'codes': x['Test Directory Clinical Indication'].split('|'),
            'date': date_str_to_datetime(x['Received Specimen Date Time'])
        } for x in clarity_df.to_dict('records')
    }

    return sample_code_mapping


def parse_sample_identifiers(reports) -> list:
    """
    Return the required fields from the xlsx report file name as a dict
    per sample of the project, full samplename, instrument ID and the
    specimen ID

    Parameters
    ----------
    reports : list
        list of file object dicts

    Returns
    -------
    list
        list of dicts with required sample details
    """
    # TODO - consider here if a report is not named correctly it could
    # introduce some messiness, do we just throw it out, raise a warning
    # or raise an error?
    samples = [
        {
            'project': x['project'],
            'sample': x['describe']['name'].split('_')[0],
            'instrument_id': x['describe']['name'].split('-')[0],
            'specimen_id': x['describe']['name'].split('-')[1]
        } for x in reports
    ]

    # ensure we don't have duplicates from multiple reports jobs
    samples = [dict(s) for s in set(frozenset(d.items()) for d in samples)]

    return samples
