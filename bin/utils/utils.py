"""
General utility functions
"""
import concurrent
from datetime import datetime
import re


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
        concurrent_jobs = {
            executor.submit(func, item) for item in items
        }

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
        r"^2[3|4|5]", date
    ), "Date provided does not seem valid"

    # split parts of date out, removing leading 0 (required for datetime)
    year, month, day = [
        int(date[i : i + 2].lstrip("0")) for i in range(0, len(date), 2)
    ]

    start = datetime(year=int(f"20{year}"), month=month, day=day)

    return (datetime.now() - start).days

