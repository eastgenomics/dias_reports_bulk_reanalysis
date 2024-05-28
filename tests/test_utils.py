from datetime import datetime, timedelta
import unittest
from unittest.mock import patch

import pytest

from bin.utils.utils import date_str_to_datetime


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
        converted_date = date_str_to_datetime('230516')
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
                date_str_to_datetime(valid)


    def test_invalid_date_strings_raise_assertion_error(self):
        """
        Test that when either invalid length or string not of year 2021
        -> 2029 is passed that an AssertionError is correctly raised
        """
        invalid_strings = ["2353", "1", "2306071"]

        for invalid in invalid_strings:
            with self.subTest() and pytest.raises(AssertionError):
                date_str_to_datetime(invalid)

