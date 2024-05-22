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
        r"^2[0-9]", date
    ), "Date provided does not seem valid"

    # split parts of date out, removing leading 0 (required for datetime)
    year, month, day = [
        int(date[i : i + 2].lstrip("0")) for i in range(0, len(date), 2)
    ]

    print(f"Parsed provided date string {date} -> {day}/{month}/20{year}")

    start = datetime(year=int(f"20{year}"), month=month, day=day)

    assert start < datetime.now(), "Provided date in the future"

    return (datetime.now() - start).days


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


def group_samples_by_project(samples, projects) -> dict:
    """
    Group the list of sample reports by the project they are from.

    Returns as the structure: [
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


def add_test_codes_back_to_samples(sample_codes, project_samples) -> dict:
    """
    Add in the test codes as an additional key for each sample to the
    project_samples dict

    Parameters
    ----------
    sample_codes : dict
        mapping of specimen ID to test codes from Clarity
    project_samples : dict
        per project sample data to add test codes to

    Returns
    -------
    dict
        per project sample data with test codes added
    """
    project_samples_with_codes = deepcopy(project_samples)

    for project_id, project_data in project_samples.items():
        for idx, sample_data in enumerate(project_data['samples']):
            codes = list(set(sample_codes.get(sample_data['specimen_id'])))

            if not codes:
                # this shouldn't happen since we've taken the specimen ID
                # from the sample codes dict to make the project_samples dict
                # TODO - do something here useful
                print('oh no')

            project_samples_with_codes[project_id]['samples'][idx]['codes'] = codes
            print(project_samples_with_codes[project_id]['samples'][idx])

    return project_samples_with_codes


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
        dict mapping specimen ID to test code(s)
    """
    clarity_df = pd.read_excel(export_file)

    clarity_df['Specimen Identifier'] = clarity_df[
        'Specimen Identifier'].str.replace('SP-', '')

    # remove any cancelled and pending samples
    clarity_df = clarity_df[clarity_df['Test Validation Status'] == 'Resulted']

    clarity_df = clarity_df[
        clarity_df['Test Directory Clinical Indication'] != 'Research Use']

    clarity_df['Test Directory Clinical Indication'].fillna(value='', inplace=True)

    # generate list of specimen ID and test code
    # TODO - need to figure out if to use the Clinical Indication column
    # to get the test code from or the Test Code column, they seem to differ
    # so need to know which is correct
    samples_to_codes = (clarity_df[[
        'Specimen Identifier', 'Test Directory Clinical Indication'
    ]].to_records(index=False)).tolist()

    # generate mapping of specimen ID to list of test codes
    sample_code_mapping = defaultdict(list)

    for sample in samples_to_codes:
        sample_code_mapping[sample[0]].extend(str(sample[1]).split('|'))

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
