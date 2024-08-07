from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import pandas as pd
import pytest

from bin.utils import utils
from tests import TEST_DATA_DIR


class TestCallInParallel(unittest.TestCase):
    """
    Tests for utils.call_in_parallel

    Function takes another function name and iterable as input, and calls
    the function for each item in parallel. This is primarily used for
    querying dxpy in parallel.
    """

    @patch('bin.utils.utils.date_str_to_datetime')
    def test_number_of_calls(self, mock_date):
        """
        Test that the specific function is called the correct number of times
        """
        utils.call_in_parallel(
            utils.date_str_to_datetime,
            ['230101', '230503', '240601']
        )

        assert mock_date.call_count == 3, (
            'function not called correctly in parallel'
        )


    def test_output_correct(self):
        """
        Test that the given function is correctly called and the output
        is as expected
        """
        returned_output = utils.call_in_parallel(
            utils.date_str_to_datetime,
            ['230101', '230503', '240601']
        )

        expected_output = [
            datetime(year=2023, month=1, day=1),
            datetime(year=2023, month=5, day=3),
            datetime(year=2024, month=6, day=1)
        ]

        assert sorted(returned_output) == expected_output, (
            'parallel called function output incorrect'
        )


    @patch('bin.utils.utils.date_str_to_datetime')
    def test_exception_raised_if_called_function_raises_exception(self, mock_date):
        """
        Test if the called function raises an error that this is correctly
        passed back to the ThreadPool and an exception raised
        """
        mock_date.side_effect = AssertionError('test internal error')

        with pytest.raises(AssertionError, match='test internal error'):
            utils.call_in_parallel(utils.date_str_to_datetime, ['230101'])


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


class TestFilterNonUniqueSpecimenIds(unittest.TestCase):
    """
    Tests for utils.filter_non_unique_specimen_ids

    Function filters through the list of sample details parsed from
    report jobs to ensure there is only one per specimen ID
    """
    # data as returned from utils.parse_sample_identifiers
    unique_specimen_sample_data = [
        {
            "project": "project-xxx",
            'sample': '111111-23251R0041',
            'instrument_id': '111111',
            'specimen_id': '23251R0041',
            'codes': ['R134'],
            'date': datetime(2023, 9, 22, 0, 0)
        },
        {
            "project": "project-xxx",
            'sample': '222222-23251R0042',
            'instrument_id': '222222',
            'specimen_id': '23251R0042',
            'codes': ['R134'],
            'date': datetime(2023, 10, 25, 0, 0)
        },
        {
            "project": "project-yyy",
            'sample': '3333333-23251R003',
            'instrument_id': '333333',
            'specimen_id': '23251R0043',
            'codes': ['R134'],
            'date': datetime(2023, 3, 4, 0, 0)
        },
        {
            "project": "project-zzz",
            'sample': '444444-23251R0044',
            'instrument_id': '444444',
            'specimen_id': '23251R0044',
            'codes': ['R134'],
            'date': datetime(2023, 2, 27, 0, 0)
        }
    ]

    def test_all_unique_correctly_identified(self):
        """
        Test where all specimen IDs are unique that the same list of data
        is returned, and no non-unique are identified
        """
        unique, non_unique = utils.filter_non_unique_specimen_ids(
            self.unique_specimen_sample_data
        )

        with self.subTest():
            assert unique == self.unique_specimen_sample_data, (
                "Unique samples wrongly returned"
            )

        with self.subTest():
            assert not non_unique, "Non unique samples wrongly identified"


    def test_non_unique_specimen_correctly_identified(self):
        """
        Test where there are non-unique specimen identifiers across
        projects that these are correctly returned
        """
        # add in a duplicate specimen ID in another project
        non_unique_sample_data = deepcopy(self.unique_specimen_sample_data)
        non_unique_sample_data.append(
            {
                "project": "project-xxx",
                'sample': '111111-23251R0044',
                'instrument_id': '1111111',
                'specimen_id': '23251R0044',
                'codes': ['R134'],
                'date': datetime(2023, 2, 27, 0, 0)
            }
        )

        unique, non_unique = utils.filter_non_unique_specimen_ids(
            non_unique_sample_data
        )

        with self.subTest():
            expected_non_unique = {
                '23251R0044': [
                    {
                        "project": "project-zzz",
                        'sample': '444444-23251R0044',
                        'instrument_id': '444444',
                        'specimen_id': '23251R0044',
                        'codes': ['R134'],
                        'date': datetime(2023, 2, 27, 0, 0)
                    },
                    {
                        "project": "project-xxx",
                        'sample': '111111-23251R0044',
                        'instrument_id': '1111111',
                        'specimen_id': '23251R0044',
                        'codes': ['R134'],
                        'date': datetime(2023, 2, 27, 0, 0)
                    }
                ]
            }

            assert non_unique == expected_non_unique, (
                "Non unique specimens not correctly identified"
            )

        with self.subTest():
            assert unique == self.unique_specimen_sample_data[:-1], (
                "unique samples wrongly idenfitied where non-unique are present"
            )


class TestFilterClaritySamplesWithNoReports(unittest.TestCase):
    """
    Tests for utils.filter_clarity_samples_with_no_reports

    Simple function to highlight if any samples from the Clarity sample
    list have no reports in DNAnexus and therefore will not be run again
    """
    samples_with_report_data = [
        {
            "project": "project-xxx",
            'sample': '111111-23251R0041',
            'instrument_id': '111111',
            'specimen_id': '23251R0041',
            'codes': ['R134'],
            'date': datetime(2023, 9, 22, 0, 0)
        },
        {
            "project": "project-xxx",
            'sample': '222222-23251R0042',
            'instrument_id': '222222',
            'specimen_id': '23251R0042',
            'codes': ['R134'],
            'date': datetime(2023, 10, 25, 0, 0)
        }
    ]

    # data as parsed from Clarity, with an additional sample to the ones
    # with reports
    clarity_data = {
        '23251R0041': {
            'codes': ['R134'],
            'date': datetime(2023, 9, 22, 0, 0)
        },
        '23251R0042': {
            'codes': ['R144'],
            'date': datetime(2023, 10, 25, 0, 0)
        },
        '23251R0043': {
            'codes': ['R154'],
            'date': datetime(2023, 3, 4, 0, 0)
        }
    }


    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_correct_prints(self):
        """
        Test that the prints of total samples with / without reports
        is correct
        """
        utils.filter_clarity_samples_with_no_reports(
            clarity_samples=self.clarity_data,
            samples_w_reports=self.samples_with_report_data
        )

        stdout = self.capsys.readouterr().out

        with self.subTest("Expected with reports"):
            expected_with_reports = (
                "Total samples available to run reports for: 2"
            )

            assert expected_with_reports in stdout

        with self.subTest("Expected without reports"):
            expected_without_reports = (
                "Total no. of outstanding samples from Clarity with no prior "
                "reports in DNAnexus: 1"
            )

            assert expected_without_reports in stdout


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


@patch('bin.utils.utils.call_in_parallel')
class TestGroupDxObjectsByProject(unittest.TestCase):
    """
    Tests for utils.group_dx_objects_by_project

    Function takes a list of DXObjects (i.e. files) and split them to a
    dict by their project
    """
    # example dx objects split between 2 projects
    dx_objects = [
        {
            'id': 'file-xxx',
            'project': 'project-aaa'
        },
        {
            'id': 'file-yyy',
            'project': 'project-aaa'
        },
        {
            'id': 'file-zzz',
            'project': 'project-bbb'
        }
    ]

    def test_expected_structure_returned(self, mock_parallel_describe):
        """
        Test that the expected dic structure is returned
        """
        mock_parallel_describe.return_value = [
            {
                'id': 'project-aaa',
                'name': 'Project-A'
            },
            {
                'id': 'project-bbb',
                'name': 'Project-B'
            }
        ]

        returned_objects = utils.group_dx_objects_by_project(self.dx_objects)

        expected_structure = {
            'project-aaa': {
                'project_name': 'Project-A',
                'items': [
                    {'id': 'file-xxx', 'project': 'project-aaa'},
                    {'id': 'file-yyy', 'project': 'project-aaa'}
                ]
            },
            'project-bbb': {
                'project_name': 'Project-B',
                'items': [
                    {'id': 'file-zzz', 'project': 'project-bbb'}
                ]
            }
        }

        assert returned_objects == expected_structure, (
            'returned structure not as expected'
        )


    def test_correct_projects_provided_to_describe(self, mock_parallel_describe):
        """
        Test that the unique list of project IDs correctly passed to
        utils.call_in_parallel (that in turn calls dxpy.describe)
        """
        mock_parallel_describe.return_value = [
            {
                'id': 'project-aaa',
                'name': 'Project-A'
            },
            {
                'id': 'project-bbb',
                'name': 'Project-B'
            }
        ]

        utils.group_dx_objects_by_project(self.dx_objects)

        expected_args = ['project-aaa', 'project-bbb']

        assert sorted(
            mock_parallel_describe.call_args[0][1]
        ) == expected_args, (
            'unique list of project IDs not provided as expected'
        )


class TestAddClarityDataBackToSamples(unittest.TestCase):
    """
    Tests for utils.add_clarity_data_back_to_samples.

    Function takes the sample identifiers parsed from report jobs and
    selects back the test codes and booked dates from the Clarity data
    for each specimen ID
    """
    sample_data = [
        {
            "project": "project-xxx",
            'sample': '111111-23251R0041',
            'instrument_id': '111111',
            'specimen_id': '23251R0041',
        },
        {
            "project": "project-xxx",
            'sample': '222222-23251R0042',
            'instrument_id': '222222',
            'specimen_id': '23251R0042',
        },
        {
            "project": "project-yyy",
            'sample': '3333333-23251R0043',
            'instrument_id': '333333',
            'specimen_id': '23251R0043',
        },
        {
            "project": "project-zzz",
            'sample': '444444-23251R0044',
            'instrument_id': '444444',
            'specimen_id': '23251R0044',
        }
    ]

    # mapping of specimen ID -> test codes and booked date as returned
    # from utils.parse_clarity_export
    clarity_data = {
        '23251R0041': {
            'codes': ['R134'],
            'date': datetime(2023, 9, 22, 0, 0)
        },
        '23251R0042': {
            'codes': ['R144'],
            'date': datetime(2023, 10, 25, 0, 0)
        },
        '23251R0043': {
            'codes': ['R154'],
            'date': datetime(2023, 3, 4, 0, 0)
        },
        '23251R0044': {
            'codes': ['R164'],
            'date': datetime(2023, 2, 27, 0, 0)
        },
    }

    def test_codes_and_date_added_correctly(self):
        """
        Test that the test codes and date added correctly for each sample
        from the Clarity data
        """
        expected_output = [
            {
                "project": "project-xxx",
                'sample': '111111-23251R0041',
                'instrument_id': '111111',
                'specimen_id': '23251R0041',
                'codes': ['R134'],
                'date': datetime(2023, 9, 22, 0, 0)
            },
            {
                "project": "project-xxx",
                'sample': '222222-23251R0042',
                'instrument_id': '222222',
                'specimen_id': '23251R0042',
                'codes': ['R144'],
                'date': datetime(2023, 10, 25, 0, 0)
            },
            {
                "project": "project-yyy",
                'sample': '3333333-23251R0043',
                'instrument_id': '333333',
                'specimen_id': '23251R0043',
                'codes': ['R154'],
                'date': datetime(2023, 3, 4, 0, 0)
            },
            {
                "project": "project-zzz",
                'sample': '444444-23251R0044',
                'instrument_id': '444444',
                'specimen_id': '23251R0044',
                'codes': ['R164'],
                'date': datetime(2023, 2, 27, 0, 0)
            }
        ]

        returned_output = utils.add_clarity_data_back_to_samples(
            samples=self.sample_data,
            clarity_data=self.clarity_data
        )

        assert expected_output == returned_output, (
            "Clarity test codes and dates incorrectly added"
        )


    def test_error_raised_if_specimen_not_in_clarity_data(self):
        """
        Test that a RuntimeError is correctly raised if the specimen
        ID is missing from the Clarity data
        """
        clarity_missing_sample = deepcopy(self.clarity_data)
        clarity_missing_sample.pop('23251R0041')

        with pytest.raises(RuntimeError):
            utils.add_clarity_data_back_to_samples(
                samples=self.sample_data,
                clarity_data=clarity_missing_sample
            )


class TestLimitSamples(unittest.TestCase):
    """
    Tests for utils.limit_samples

    Function takes in the list of samples and an integer limit and / or
    start / end date for which to restrict retaining samples. The
    booked in date from Clarity is used for date range restriction.
    """
    # data as returned from utils.parse_sample_identifiers
    sample_data = [
        {
            "project": "project-xxx",
            'sample': '111111-23251R0041',
            'instrument_id': '111111',
            'specimen_id': '23251R0041',
            'codes': ['R134'],
            'date': datetime(2023, 9, 22, 0, 0)
        },
        {
            "project": "project-xxx",
            'sample': '222222-23251R0042',
            'instrument_id': '222222',
            'specimen_id': '23251R0042',
            'codes': ['R134'],
            'date': datetime(2023, 10, 25, 0, 0)
        },
        {
            "project": "project-yyy",
            'sample': '3333333-23251R0043',
            'instrument_id': '333333',
            'specimen_id': '23251R0043',
            'codes': ['R134'],
            'date': datetime(2023, 3, 4, 0, 0)
        },
        {
            "project": "project-zzz",
            'sample': '444444-23251R0044',
            'instrument_id': '444444',
            'specimen_id': '23251R0044',
            'codes': ['R134'],
            'date': datetime(2023, 2, 27, 0, 0)
        }
    ]

    def test_integer_limit_works(self):
        """
        Test that limit parameter works as expected, this should take
        the oldest n samples from the provided list.

        With limit of 2 we will expect to keep samples '3333333-23251R0043'
        and '444444-23251R0044'
        """
        limited_samples = utils.limit_samples(
            samples=self.sample_data,
            limit=2
        )

        expected_samples = [
            {
                "project": "project-zzz",
                'sample': '444444-23251R0044',
                'instrument_id': '444444',
                'specimen_id': '23251R0044',
                'codes': ['R134'],
                'date': datetime(2023, 2, 27, 0, 0)
            },
            {
                "project": "project-yyy",
                'sample': '3333333-23251R0043',
                'instrument_id': '333333',
                'specimen_id': '23251R0043',
                'codes': ['R134'],
                'date': datetime(2023, 3, 4, 0, 0)
            }
        ]

        assert limited_samples == expected_samples, (
            'limiting samples with integer limit incorrect'
        )


    def test_limit_with_start_date(self):
        """
        Test when only start date provided limiting works as expected.

        Set date to 1st June => retain first 2 samples
        """
        limited_samples = utils.limit_samples(
            samples=self.sample_data,
            start='230601'
        )

        expected_samples = [
            {
                "project": "project-xxx",
                'sample': '111111-23251R0041',
                'instrument_id': '111111',
                'specimen_id': '23251R0041',
                'codes': ['R134'],
                'date': datetime(2023, 9, 22, 0, 0)
            },
            {
                "project": "project-xxx",
                'sample': '222222-23251R0042',
                'instrument_id': '222222',
                'specimen_id': '23251R0042',
                'codes': ['R134'],
                'date': datetime(2023, 10, 25, 0, 0)
            }
        ]

        assert limited_samples == expected_samples, (
            "incorrect samples retained with start date limit"
        )


    def test_limit_with_end_date(self):
        """
        Test when only end date provided limiting works as expected.

        Set date to 1st June => retain last 2 samples
        """
        limited_samples = utils.limit_samples(
            samples=self.sample_data,
            end='230601'
        )

        expected_samples = [
            {
                "project": "project-zzz",
                'sample': '444444-23251R0044',
                'instrument_id': '444444',
                'specimen_id': '23251R0044',
                'codes': ['R134'],
                'date': datetime(2023, 2, 27, 0, 0)
            },
            {
                "project": "project-yyy",
                'sample': '3333333-23251R0043',
                'instrument_id': '333333',
                'specimen_id': '23251R0043',
                'codes': ['R134'],
                'date': datetime(2023, 3, 4, 0, 0)
            }
        ]

        assert limited_samples == expected_samples, (
            "incorrect samples retained with end date limit"
        )


    def test_limit_with_start_and_end_date(self):
        """
        Test that with a start and end date provided that limiting works
        as expected.

        Set start to 1st March and end date to 1st June => retain just
        sample '3333333-23251R0043' booked in on 4th March
        """
        limited_samples = utils.limit_samples(
            samples=self.sample_data,
            start='230301',
            end='230601'
        )

        expected_samples = [
            {
                "project": "project-yyy",
                'sample': '3333333-23251R0043',
                'instrument_id': '333333',
                'specimen_id': '23251R0043',
                'codes': ['R134'],
                'date': datetime(2023, 3, 4, 0, 0)
            }
        ]

        assert limited_samples == expected_samples, (
            'incorrect samples retained with start and end date'
        )


    def test_limit_with_integer_and_date_range(self):
        """
        Test that when integer and date range provided limiting works
        as expected.

        With limit of 2, start 1st March and end 1st December that only
        samples '3333333-23251R0043' and '111111-23251R0041' are
        retained
        """
        limited_samples = utils.limit_samples(
            samples=self.sample_data,
            limit=2,
            start='230301',
            end='231201'
        )

        expected_samples = [
            {
                "project": "project-yyy",
                'sample': '3333333-23251R0043',
                'instrument_id': '333333',
                'specimen_id': '23251R0043',
                'codes': ['R134'],
                'date': datetime(2023, 3, 4, 0, 0)
            },
            {
                "project": "project-xxx",
                'sample': '111111-23251R0041',
                'instrument_id': '111111',
                'specimen_id': '23251R0041',
                'codes': ['R134'],
                'date': datetime(2023, 9, 22, 0, 0)
            }
        ]

        assert limited_samples == expected_samples, (
            'incorrect samples retained with integer and date range limits'
        )


class TestParseConfig(unittest.TestCase):
    """
    Tests for utils.parse_config

    Function reads in the config file stored in configs/ that contains
    the manually selected CNV call jobs and Dias single paths for specific
    projects where >1 of each exists and we can't unambiguously select
    the correct one
    """
    @patch('bin.utils.utils.path.join')
    def test_correct_contents_returned(self, mock_join):
        """
        Test that the function correctly loads the json and returns the
        dicts of the cnv call job IDs and Dias single paths
        """
        mock_join.return_value = (
            f"{os.path.dirname(os.path.abspath(__file__))}"
            "/test_data/manually_selected.json"
        )

        cnv_jobs, dias_single_paths = utils.parse_config()

        expected_cnv_jobs = {
            "project-GgZyg8j47Ky5z0vBG0JBB0QJ": "job-Ggggppj47Ky46K2KZYyB7J3B",
            "project-GgJ3gf04F80JY20Gjkp0QjF4": "job-GgPYb984F80JZv63zG198VvZ",
            "project-GZk71GQ446x5YQkjzvpYFBzB": "job-GZq727Q446x28FQ74BkqBJx9",
            "project-GZ3zJBj4X0Vy0b4Y20QyG1B2": "job-GZ4q5VQ4X0Vz3jkP95Yb058J",
            "project-GXZg37j4kgGxFZ29fj3f3Yp4": "job-GXby1ZQ4kgGXQK7gyv506Xj9",
            "project-GXZg0J04BXfPFFZYYFGz42bP": "job-GXbyZ104BXf8G5296g93bvx2"
        }

        expected_dias_single_paths = {
            "project-GgXvB984QX3xF6qkPK4Kp5xx": "/output/CEN-240304_1257",
            "project-Ggyb2G84zJ4363x2JqfGgb6J": "/output/CEN-240322_0936"
        }

        with self.subTest():
            assert cnv_jobs == expected_cnv_jobs, "CNV call jobs incorrect"

        with self.subTest():
            assert dias_single_paths == expected_dias_single_paths, (
                "Dias single paths incorrect"
            )


class TestParseClarityExport(unittest.TestCase):
    """
    Tests for utils.parse_clarity_export

    Function reads in an export from Clarity in xlsx format, parsing out
    the specimen ID, booked tests and booked in date. This is then
    returned as a structured dict containing the required information.
    """
    clarity_export_file = (
        f"{os.path.dirname(os.path.abspath(__file__))}"
        "/test_data/example_clarity_export.xlsx"
    )

    def test_correctly_parsed(self):
        """
        Test that the export is correctly parsed.

        The test data contains 5 samples, 4 of which should be parsed as
        they are at the stage `Resulted`, and one should be excluded that
        has the state `Cancelled`.

        Other behaviour that is expected:
            - `SP-` stripped from specimen IDs
            - received date parsed as valid datetime object
            - data returned as dict mapping specimen ID -> test codes
                and received date
        """

        parsed_export = utils.parse_clarity_export(self.clarity_export_file)

        expected_format = {
            "24095R01111": {
                "codes": ["R134.1", "R134.2"],
                "date": datetime(2024, 4, 8, 0, 0)
            },
            "24053R02222": {
                "codes": ["R134.1", "R134.2"],
                "date": datetime(2024, 2, 27, 0, 0)
            },
            "24057R03333": {
                "codes": ["R414.1", "R414.2"],
                "date": datetime(2024, 2, 26, 0, 0)
            }
        }

        assert parsed_export == expected_format, (
            "clarity export incorrectly parsed"
        )


class TestParseSampleIdentifiers(unittest.TestCase):
    """
    Tests for utils.parse_sample_identifiers

    Function takes a list of describe objects from dxpy.find_data_objects
    for xlsx reports found from a given set of specimen IDs, and parses
    out the instrument ID and project ID for each. This is because the
    Clarity data doesn't contain the instrument ID so we are getting this
    from the reports job data.
    """
    # minimal return from dxpy.find_data_objects as would be returned
    # from the call in dx_manage.get_xlsx_reports
    find_data_return = [
        {
            "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
            "id": "file-GkBzKBj4jzpkvxq1fqqv1Z4g",
            "describe": {
                "id": "file-GkBzKBj4jzpkvxq1fqqv1Z4g",
                "name": "111111111-12345R6789-24NGCEN41-9527-F-99347387_R208.1_CNV_1.xlsx",
                "createdBy": {
                    "user": "user-1",
                    "job": "job-GkBz2b04fz4qVZYZ78JpzxzZ",
                    "executable": "app-Gj6YVp841jVJZZbXV9xXGybk"
                },
                "archivalState": "live"
            }
        },
        {
            "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
            "id": "file-GkBypqj4X9G88J6j360gQJbB",
            "describe": {
                "id": "file-GkBypqj4X9G88J6j360gQJbB",
                "name": "222222222-9876R54321-24NGCEN41-9527-F-99347387_R45.1_SNV_1.xlsx",
                "createdBy": {
                    "user": "user-1",
                    "job": "job-GkBy0k04fz4Y4BG6yv38XkzQ",
                    "executable": "app-Gj6YVp841jVJZZbXV9xXGybk"
                },
                "archivalState": "live"
            }
        },
        {
            "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
            "id": "file-GkBypqj4X9G88hfbf7y7bdbdvwlA",
            "describe": {
                "id": "file-GkBypqj4X9G88hfbf7y7bdbdvwlA",
                "name": "333333333-9876R54321-24NGCEN41-9527-F-99347387_HGNC:1234_SNV_1.xlsx",
                "createdBy": {
                    "user": "user-1",
                    "job": "job-GkBy0k04fz4Y4BG6yv38XkzQ",
                    "executable": "app-Gj6YVp841jVJZZbXV9xXGybk"
                },
                "archivalState": "live"
            }
        }
    ]

    def test_identifiers_parsed_correctly(self):
        """
        Test that for each item we correctly return:
            - project ID
            - full samplename
            - instrument ID
            - specimen ID
        """
        expected_return = [
            {
                "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
                "sample": "111111111-12345R6789-24NGCEN41-9527-F-99347387",
                "instrument_id": "111111111",
                "specimen_id": "12345R6789"
            },
            {
                "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
                "sample": "222222222-9876R54321-24NGCEN41-9527-F-99347387",
                "instrument_id": "222222222",
                "specimen_id": "9876R54321"
            },
            {
                "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
                "sample": "333333333-9876R54321-24NGCEN41-9527-F-99347387",
                "instrument_id": "333333333",
                "specimen_id": "9876R54321"
            }
        ]

        parsed_return = utils.parse_sample_identifiers(self.find_data_return)

        assert expected_return == parsed_return, (
            "sample identifiers incorrectly parsed from reports data"
        )


    def test_duplicates_correctly_returned(self):
        """
        Test that for each item we correctly return:
            - project ID
            - full samplename
            - instrument ID
            - specimen ID
        """
        # copy and add in an additional report return for the same sample
        # that already exists
        find_data_copy = deepcopy(self.find_data_return)
        find_data_copy.append(
            {
                "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
                "id": "file-GkBzX88477Zqq5G74ff7qVV5",
                "describe": {
                    "id": "file-GkBzX88477Zqq5G74ff7qVV5",
                    "name": "111111111-12345R6789-24NGCEN41-9527-F-99347387_R208.1_SNV_1.xlsx",
                    "createdBy": {
                        "user": "user-1",
                        "job": "job-GkBz2f04fz4g4XpK4ykgZq3p",
                        "executable": "app-Gj6YVp841jVJZZbXV9xXGybk"
                    },
                    "archivalState": "live"
                }
            }
        )

        expected_return = [
            {
                "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
                "sample": "111111111-12345R6789-24NGCEN41-9527-F-99347387",
                "instrument_id": "111111111",
                "specimen_id": "12345R6789"
            },
            {
                "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
                "sample": "222222222-9876R54321-24NGCEN41-9527-F-99347387",
                "instrument_id": "222222222",
                "specimen_id": "9876R54321"
            },
            {
                "project": "project-Gk7bv204fz4YVzb8Yp0BYjG2",
                "sample": "333333333-9876R54321-24NGCEN41-9527-F-99347387",
                "instrument_id": "333333333",
                "specimen_id": "9876R54321"
            }
        ]

        parsed_return = utils.parse_sample_identifiers(self.find_data_return)

        assert expected_return == parsed_return, (
            "sample identifiers incorrectly parsed from reports data"
        )


    def test_invalid_report_names_raise_runtime_error(self):
        """
        Test that where a file name we use to parse identifiers from
        doesn't pass the basic regex that an error is correctly raised
        """
        # minimal set of describe objects with invalid identifiers
        invalid_names = [
            {
                'describe': {
                    'name': 'X12345.xlsx'
                }
            },
            {
                'describe': {
                    'name': 'X12345-1234.xlsx'
                }
            },
            {
                'describe': {
                    'name': 'X12345_1234.xlsx'
                }
            },
        ]

        for sample in invalid_names:
            error = (
                "ERROR: xlsx reports found that specimen and instrument "
                f"IDs could not be parsed from: {sample['describe']['name']}"
            )

            with self.subTest() and pytest.raises(RuntimeError, match=error):
                utils.parse_sample_identifiers([sample])


class TestSplitGenePanelsTestCodes():
    """
    Tests for utils.split_genepanels_test_codes()

    Function takes the read in genepanels file and splits out the test code
    that prefixes the clinical indication (i.e. R337.1 -> R337.1_CADASIL_G)
    """
    # read in genepanels file in the same manner as utils.parse_genepanels()
    # up to the point of calling split_gene_panels_test_codes()
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels = pd.DataFrame(
            [x.split('\t') for x in genepanels_data],
            columns=['indication', 'panel_name', 'hgnc_id']
        )
        genepanels.drop(columns=['hgnc_id'], inplace=True)  # chuck away HGNC ID
        genepanels.drop_duplicates(keep='first', inplace=True)
        genepanels.reset_index(inplace=True)


    def test_genepanels_unchanged_by_splitting(self):
        """
        Test that no rows get added or removed
        """
        panel_df = utils.split_genepanels_test_codes(self.genepanels)

        current_indications = self.genepanels['indication'].tolist()
        split_indications = panel_df['indication'].tolist()

        assert current_indications == split_indications, (
            'genepanels indications changed when splitting test codes'
        )

    def test_splitting_r_code(self):
        """
        Test splitting of R code from a clinical indication works
        """
        panel_df = utils.split_genepanels_test_codes(self.genepanels)
        r337_code = panel_df[panel_df['indication'] == 'R337.1_CADASIL_G']

        assert r337_code['test_code'].tolist() == ['R337.1'], (
            "Incorrect R test code parsed from clinical indication"
        )

    def test_splitting_c_code(self):
        """
        Test splitting of C code from a clinical indication works
        """
        panel_df = utils.split_genepanels_test_codes(self.genepanels)
        c1_code = panel_df[panel_df['indication'] == 'C1.1_Inherited Stroke']

        assert c1_code['test_code'].tolist() == ['C1.1'], (
            "Incorrect C test code parsed from clinical indication"
        )

 
    def test_catch_multiple_indication_for_one_test_code(self):
        """
        We have a check that if a test code links to more than one clinical
        indication (which it shouldn't), we can add in a duplicate and test
        that this gets caught
        """
        genepanels_copy = deepcopy(self.genepanels)
        genepanels_copy = pd.concat([genepanels_copy,
            pd.DataFrame([{
                'test_code': 'R337.1',
                'indication': 'R337.1_CADASIL_G_COPY',
                'panel_name': 'R337.1_CADASIL_G_COPY'
            }])
        ])

        with pytest.raises(RuntimeError):
            utils.split_genepanels_test_codes(genepanels_copy)


class TestValidateTestCodes(unittest.TestCase):
    """
    Tests for utils.validate_test_codes()

    Function parses through all samples -> test codes to check they are
    valid against the genepanels file, this is to ensure nothing will
    fail launching reports jobs due to having invalid codes in booked
    """
    # read in genepanels file in the same manner as
    # dx_manage.parse_genepanels() up to the point of calling
    # split_gene_panels_test_codes()
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        genepanels_data = file_handle.read().splitlines()
        genepanels = pd.DataFrame(
            [x.split('\t') for x in genepanels_data],
            columns=['indication', 'panel_name', 'hgnc_id']
        )
        genepanels.drop(columns=['hgnc_id'], inplace=True)  # chuck away HGNC ID
        genepanels.drop_duplicates(keep='first', inplace=True)
        genepanels.reset_index(inplace=True)


    sample_data = [
        {
            "project": "project-xxx",
            'sample': '111111-23251R0041',
            'instrument_id': '111111',
            'specimen_id': '23251R0041',
            'codes': ['R109.3'],
            'date': datetime(2023, 9, 22, 0, 0)
        },
        {
            "project": "project-xxx",
            'sample': '222222-23251R0042',
            'instrument_id': '222222',
            'specimen_id': '23251R0042',
            'codes': ['R134.1'],
            'date': datetime(2023, 10, 25, 0, 0)
        },
        {
            "project": "project-yyy",
            'sample': '3333333-23251R0043',
            'instrument_id': '333333',
            'specimen_id': '23251R0043',
            'codes': ['R146.2'],
            'date': datetime(2023, 3, 4, 0, 0)
        },
        {
            "project": "project-zzz",
            'sample': '444444-23251R0044',
            'instrument_id': '444444',
            'specimen_id': '23251R0044',
            'codes': ['HGNC:1234'],
            'date': datetime(2023, 2, 27, 0, 0)
        }
    ]


    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_error_not_raised_on_valid_codes(self):
        """
        If all test codes are valid the function should just print
        to stdout and not raise an error
        """
        utils.validate_test_codes(
            all_sample_data=self.sample_data, genepanels=self.genepanels
        )

        expected_stdout = 'All sample test codes valid!'

        assert expected_stdout in self.capsys.readouterr().out, (
            'expected stdout incorrect'
        )


    def test_error_raised_when_sample_has_no_tests(self):
        """
        Test we raise an error if a sample has no test codes booked against it
        """
        # drop test codes for a booked sample
        sample_data_copy = deepcopy(self.sample_data)
        sample_data_copy[0]['codes'] = []

        with pytest.raises(RuntimeError, match=r"No tests booked for sample"):
            utils.validate_test_codes(
                all_sample_data=sample_data_copy, genepanels=self.genepanels
            )


    def test_error_raised_when_sample_has_invalid_test_code(self):
        """
        RuntimeError should be raised if an invalid test code is provided
        in the manifest, check that the correct error is returned
        """
        # add in an invalid test code to a booked sample
        sample_data_copy = deepcopy(self.sample_data)
        sample_data_copy[0]['codes'].append('invalidTestCode')

        with pytest.raises(RuntimeError, match=r"invalidTestCode"):
            utils.validate_test_codes(
                all_sample_data=sample_data_copy, genepanels=self.genepanels
            )


    def test_error_not_raised_when_research_use_test_code_present(self):
        """
        Sometimes from Epic 'Research Use' can be present in the Test Codes
        column, we want to skip these as they're not a valid test code and
        not raise an error
        """
        # add in different forms of 'Research Use' as a test code to a
        # manifest sample
        sample_data_copy = deepcopy(self.sample_data)
        sample_data_copy[0]['codes'].extend([
            'Research Use', 'ResearchUse', 'researchUse', 'research use'
        ])

        utils.validate_test_codes(
            all_sample_data=sample_data_copy, genepanels=self.genepanels
        )

        expected_stdout_success = 'All sample test codes valid!'

        expected_stdout_warning = (
            "WARNING: 111111-23251R0041 booked for 'Research Use' test, "
            "skipping this test code and continuing...\n"
            "WARNING: 111111-23251R0041 booked for 'ResearchUse' test, "
            "skipping this test code and continuing...\n"
            "WARNING: 111111-23251R0041 booked for 'researchUse' test, "
            "skipping this test code and continuing...\n"
            "WARNING: 111111-23251R0041 booked for 'research use' test, "
            "skipping this test code and continuing..."
        )

        stdout = self.capsys.readouterr().out

        with self.subTest():
            assert expected_stdout_success in stdout, (
                'expected stdout success incorrect'
            )

        with self.subTest():
            assert expected_stdout_warning in stdout, (
                'expected stdout warnings incorrect'
            )


class TestWriteToLog(unittest.TestCase):
    """
    Tests for utils.write_to_log

    Function takes a filename of a log to write to along with a key and
    list of job IDs, and updates (or creates a new) the given log file.
    """
    def test_assertion_error_raised_if_non_json_specified(self):
        """
        Test that if a non-json file is provided that an AssertionError
        is raised
        """
        with pytest.raises(AssertionError):
            utils.write_to_log(
                log_file="my_log.txt",
                key='foo',
                job_ids=['bar']
            )


    @patch('bin.utils.utils.path')
    def test_log_file_updated_if_already_exists(self, mock_path):
        """
        Test that if a log already exists that it is updated
        and not overwritten
        """
        # create a test file to exist
        with open('/tmp/test_config.json', 'w') as fh:
            json.dump({'foo': ['bar']}, fh)

        mock_path.abspath.return_value = '/tmp/test_config.json'

        utils.write_to_log(
            log_file="test_config.json",
            key='baz',
            job_ids=['blarg']
        )

        # test that the log file has been updated and not overwritten
        with open('/tmp/test_config.json', 'r') as fh:
            log_contents = json.load(fh)

        expected_contents = {
            'foo': ['bar'],
            'baz': ['blarg']
        }

        os.remove('/tmp/test_config.json')

        assert log_contents == expected_contents, (
            'Log contents not as expected'
        )


    @patch('bin.utils.utils.path')
    def test_log_file_created_if_not_already_exists(self, mock_path):
        """
        Test that if a log does not already exist that it is created
        """
        mock_path.abspath.return_value = '/tmp/test_config.json'
        mock_path.exists.return_value = False

        utils.write_to_log(
            log_file="test_config.json",
            key='baz',
            job_ids=['blarg']
        )

        # test that the log file has been created
        with open('/tmp/test_config.json', 'r') as fh:
            log_contents = json.load(fh)

        os.remove('/tmp/test_config.json')

        expected_contents = {
            'baz': ['blarg']
        }

        assert log_contents == expected_contents, (
            'new log file does not contain the expected contents'
        )


class TestReadFromLog(unittest.TestCase):
    """
    Tests for utils.read_from_log

    Simple function that reads in a JSON log file and returns a dict
    of the contents
    """
    def test_assertion_error_raised_on_non_json_file(self):
        """
        Test that an AssertionError is correctly raised on a non JSON
        file being provided
        """
        with pytest.raises(
            AssertionError,
            match="JSON file not provided to read from"
        ):
            utils.read_from_log('myFile.txt')


    def test_json_correctly_read(self):
        """
        Test that JSON file correctly read in and contents returned
        """
        tmp_file = f"{uuid4().hex}.json"
        with open(tmp_file, 'w') as fh:
            json.dump({'foo': 'bar'}, fh)

        returned_contents = utils.read_from_log(tmp_file)

        os.remove(tmp_file)

        assert returned_contents == {'foo': 'bar'}, (
            'contents of log file not as expected'
        )
