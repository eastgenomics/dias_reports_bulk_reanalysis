"""
General utility functions
"""

import concurrent
from datetime import datetime
import re

import pandas as pd


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


def parse_clarity_export(export_file) -> list:
    """
    Parse the xlsx export from Clarity to get the samples and test codes

    Parameters
    ----------
    export_file : str
        file name of Clarity export to parse

    Returns
    -------
    list
        list of tuples of specimen ID and test code(s)
    """
    clarity_df = pd.read_excel(export_file)

    clarity_df['Specimen Identifier'] = clarity_df[
        'Specimen Identifier'].str.replace('SP-', '')

    # remove any cancelled and pending samples
    clarity_df = clarity_df[clarity_df['Test Validation Status'] == 'Resulted']

    clarity_df = clarity_df[
        clarity_df['Test Directory Clinical Indication'] != 'Research Use']

    # generate list of specimen ID and test code
    # TODO - need to figure out if to use the Clinical Indication column
    # to get the test code from or the Test Code column, they seem to differ
    # so need to know which is correct
    samples_to_codes = clarity_df[[
        'Specimen Identifier', 'Test Directory Clinical Indication'
    ]].to_records(index=False).tolist()

    return samples_to_codes


