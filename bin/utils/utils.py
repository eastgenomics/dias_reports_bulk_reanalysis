"""
General utility functions
"""
from collections import defaultdict
import concurrent
from datetime import datetime
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


def group_samples_by_project(samples) -> dict:
    """
    Group the list of sample reports by the project they are from.

    Returns as the structure: [
        {
            'project-xxx': {
                'samples': [
                    {
                        'id': 'file-xxx'
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

    return {k: dict(v) for k, v in dict(project_samples).items()}


# def match_test_codes_to_samples(project_samples)


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
    ]].to_records(index=False))

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
    reports : _type_
        _description_

    Returns
    -------
    list
        _description_
    """
    return [
        {   'project': x['project'],
            'sample': x['describe']['name'].split('_')[0],
            'instrument_id': x['describe']['name'].split('-')[0],
            'specimen_id': x['describe']['name'].split('-')[1]
        } for x in reports
    ]
