from datetime import datetime, timedelta
import unittest
from unittest.mock import patch

import pytest

from bin.utils import utils


class TestDateStrToDatetime(unittest.TestCase):
    """
    Tests for utils.date_to_datetime

    Function takes a 6 digit string (YYMMHH) and returns this as a
    valid datetime.datetime object
    """
    def test_correct_datetime_returned(self):
        """
        Test correct datetime object returned for valid input string
        """
        converted_date = utils.date_str_to_datetime('230516')
        correct_date = datetime(year=2023, month=5, day=16)

        assert converted_date == correct_date, 'Wrong date returned'


    def test_valid_date_strings_do_not_raise_assertion(self):
        """
        Test that when valid date strings are passed that no assertion
        error is raised
        """
        # generate list of valid dates for the past few years
        valid_dates = [(
            datetime.today() - timedelta(days=x)
        ).strftime('%y%m%d') for x in range(1000)]

        for valid in valid_dates:
            with self.subTest():
                utils.date_str_to_datetime(valid)


    def test_invalid_date_strings_raise_assertion_error(self):
        """
        Test that when either invalid length or string not of year 2021
        -> 2029 is passed that an AssertionError is correctly raised
        """
        invalid_strings = ["2353", "1", "2306071"]

        for invalid in invalid_strings:
            with self.subTest() and pytest.raises(AssertionError):
                utils.date_str_to_datetime(invalid)



class TestGroupSamplesByProject(unittest.TestCase):
    """
    Tests for utils.group_samples_by_project

    Function takes the list of sample identifiers and the project they
    are from that comes from the report job details, and splits this into
    per project dictionary of samples.
    """

    # data as returned from utils.parse_sample_identifiers
    sample_data = [
        {
            "project": "project-xxx",
            'sample': '111111-23251R0047',
            'instrument_id': '111111',
            'specimen_id': '23251R0047',
            'codes': ['R134'],
            'date': datetime(2023, 9, 22, 0, 0)
        },
        {
            "project": "project-xxx",
            'sample': '222222-23251R0047',
            'instrument_id': '222222',
            'specimen_id': '23251R0047',
            'codes': ['R134'],
            'date': datetime(2023, 10, 25, 0, 0)
        },
        {
            "project": "project-yyy",
            'sample': '3333333-23251R0047',
            'instrument_id': '333333',
            'specimen_id': '23251R0047',
            'codes': ['R134'],
            'date': datetime(2023, 3, 4, 0, 0)
        },
        {
            "project": "project-zzz",
            'sample': '444444-23251R0047',
            'instrument_id': '444444',
            'specimen_id': '23251R0047',
            'codes': ['R134'],
            'date': datetime(2023, 2, 27, 0, 0)
        }
    ]

    # minimal project data as returned from dx_manage.get_projects
    project_data = {
        'project-xxx': {
            "name": "002_test_1"
        },
        "project-yyy": {
            "name": "002_test_2"
        },
        "project-zzz": {
            "name": "002_test_3"
        }
    }

    def test_correct_grouping_by_project(self):
        """
        Test that sample data is correctly grouped by project ID
        """
        returned_grouping = utils.group_samples_by_project(
            samples=self.sample_data,
            projects=self.project_data
        )

        expected_grouping = {
            "project-xxx": {
                "project_name": "002_test_1",
                "samples": [
                    {
                        "project": "project-xxx",
                        'sample': '111111-23251R0047',
                        'instrument_id': '111111',
                        'specimen_id': '23251R0047',
                        'codes': ['R134'],
                        'date': datetime(2023, 9, 22, 0, 0)
                    },
                    {
                        "project": "project-xxx",
                        'sample': '222222-23251R0047',
                        'instrument_id': '222222',
                        'specimen_id': '23251R0047',
                        'codes': ['R134'],
                        'date': datetime(2023, 10, 25, 0, 0)
                    }
                ]
            },
            "project-yyy": {
                "project_name": "002_test_2",
                "samples": [
                    {
                        "project": "project-yyy",
                        'sample': '3333333-23251R0047',
                        'instrument_id': '333333',
                        'specimen_id': '23251R0047',
                        'codes': ['R134'],
                        'date': datetime(2023, 3, 4, 0, 0)
                    }
                ]
            },
            "project-zzz": {
                "project_name": "002_test_3",
                "samples": [
                    {
                        "project": "project-zzz",
                        'sample': '444444-23251R0047',
                        'instrument_id': '444444',
                        'specimen_id': '23251R0047',
                        'codes': ['R134'],
                        'date': datetime(2023, 2, 27, 0, 0)
                    }
                ]
            }
        }

        assert returned_grouping == expected_grouping, (
            'Sample data incorrectly grouped by project'
        )