from datetime import datetime
import pytest
import unittest
from unittest.mock import Mock, patch

from bin.utils.utils import date_to_datetime


class TestDateToDatetime(unittest.TestCase):
    """
    Tests for utils.date_to_datetime

    Function takes a 6 digit string (YYMMHH) and calculates the number
    of days from then until current day for searching for projects
    """
    @patch('bin.utils.utils.datetime', wraps=datetime)
    def test_correct_no_days_returned(self, mock_datetime):
        """
        Test correct no. days returned given valid date string, days
        are between 02/03/2023 -> 03/03/2024 => 367 days
        """
        # patch over now to be a fixed date to calculate up until
        mock_datetime.now.return_value = datetime(2024, 3, 3)

        calculated_days = date_to_datetime('230302')

        assert calculated_days == 367, 'time delate calculated incorrectly'


    def test_invalid_date_strings_raise_assertion_error(self):
        """
        Test that when either invalid length or string not of year 2021
        -> 2029 is passed that an AssertionError is correctly raised
        """
        invalid_strings = ['2353', '1', '2306071']

        for invalid in invalid_strings:
            with self.subTest() and pytest.raises(AssertionError):
                date_to_datetime(invalid)


    @patch('bin.utils.utils.datetime', wraps=datetime)
    def test_date_in_future_raises_assertion_error(self, mock_datetime):
        """
        Test that when a date provided is in the future that an
        AssertionError is correctly raised
        """
        mock_datetime.now.return_value = datetime(2024, 3, 3)

        with pytest.raises(
            AssertionError,
            match='Provided date in the future'
        ):
            date_to_datetime('240606')