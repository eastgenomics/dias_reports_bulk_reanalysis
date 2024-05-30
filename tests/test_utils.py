from copy import deepcopy
from datetime import datetime, timedelta
import unittest
from unittest.mock import patch

import pytest

from bin.utils import utils


class TestCallInParallel(unittest.TestCase):
    """
    TODO
    """
    pass


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
    TODO
    """
    pass


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
    TODO
    """
    pass




class TestParseConfig(unittest.TestCase):
    """
    TODO
    """
    pass


class TestParseClarityExport(unittest.TestCase):
    """
    TODO
    """
    pass


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

