from copy import deepcopy
from datetime import datetime, timedelta
from os import path
import unittest
from unittest.mock import patch

import pytest

from bin.utils import utils


class TestCallInParallel(unittest.TestCase):
    """
    Tests for utils.call_in_paralllel

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
            'parallel called funtion output incorrect'
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
    TODO
    """
    pass


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


    def error_raised_if_specimen_not_in_clarity_data(self):
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
            f"{path.dirname(path.abspath(__file__))}"
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
    TODO
    """
    pass


class TestParseSampleIdentifiers(unittest.TestCase):
    """
    TODO
    """
    pass



