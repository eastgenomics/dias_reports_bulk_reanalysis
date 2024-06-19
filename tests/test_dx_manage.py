"""Tests for dx_manage"""
import unittest
from unittest.mock import patch

import pytest

from bin.utils import dx_manage


class TestCheckArchivalState(unittest.TestCase):
    """
    Tests for dx_manage.check_archival_state

    Function has a list of required file patterns to check the state of
    for the given list of sample IDs. These files are all found using
    dxpy.find_data_objects() to then parse the archivalState from, and
    3 lists of files are returned for live, currently unarchiving and
    unarchived files
    """
    # minimal example file detail return from dx_manage.find_in_parallel()
    # with a mix of live, unarchiving and archived files
    returned_file_details = [
        {
            "project": "project-xxx",
            "id": "file-aaa",
            "describe": {
                "archivalState": "live"
            }
        },
        {
            "project": "project-xxx",
            "id": "file-bbb",
            "describe": {
                "archivalState": "live"
            }
        },
        {
            "project": "project-xxx",
            "id": "file-ccc",
            "describe": {
                "archivalState": "unarchiving"
            }
        },
        {
            "project": "project-xxx",
            "id": "file-aaa",
            "describe": {
                "archivalState": "archived"
            }
        },
        {
            "project": "project-xxx",
            "id": "file-aaa",
            "describe": {
                "archivalState": "archived"
            }
        },
    ]

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    @patch('bin.utils.dx_manage.find_in_parallel')
    def test_all_states_mixed_returned_correctly(self, mock_find):
        """
        Test that we correctly return the given files by their states
        """
        mock_find.return_value = self.returned_file_details

        live, unarchiving, archived = dx_manage.check_archival_state(
            project='project-xxx',
            sample_data={
                'samples':[{'sample': 'sample1'}, {'sample': 'sample2'}]
            }
        )

        expected_live = [
            {
            "project": "project-xxx",
            "id": "file-aaa",
            "describe": {
                "archivalState": "live"
            }
            },
            {
                "project": "project-xxx",
                "id": "file-bbb",
                "describe": {
                    "archivalState": "live"
                }
            }
        ]

        expected_unarchiving = [
            {
                "project": "project-xxx",
                "id": "file-ccc",
                "describe": {
                    "archivalState": "unarchiving"
                }
            }
        ]

        expected_archived = [
            {
            "project": "project-xxx",
                "id": "file-aaa",
                "describe": {
                    "archivalState": "archived"
                }
            },
            {
                "project": "project-xxx",
                "id": "file-aaa",
                "describe": {
                    "archivalState": "archived"
                }
            }
        ]

        with self.subTest("live files wrongly identified"):
            assert live == expected_live

        with self.subTest("unarchiving files wrongly identified"):
            assert unarchiving == expected_unarchiving

        with self.subTest("archived files wrongly identified"):
            assert archived == expected_archived


    @patch('bin.utils.dx_manage.find_in_parallel')
    def test_correct_number_files_searched_for(self, mock_find):
        """
        When searching in DNAnexus, there are a set number of patterns
        defined in the function that are searched for each sample provided
        in the sample data.

        We will use the number of files printed to stdout as a proxy for
        the correct list of patterns being built since we don't return them.
        """
        dx_manage.check_archival_state(
            project='project-xxx',
            sample_data={
                'samples':[{'sample': 'sample1'}, {'sample': 'sample2'}]
            }
        )

        # since we pass 2 samples, we expect 2 * 8 patterns plus the
        # run level excluded intervals bed => 17 file patterns
        stdout = self.capsys.readouterr().out

        expected_stdout = "17 sample files to search for"

        assert expected_stdout in stdout, (
            "Wrong no. files identified to check archival state of"
        )


class TestUnarchiveFiles(unittest.TestCase):
    """

    """
    pass


class TestCreateFolder(unittest.TestCase):
    """

    """
    pass


class TestFindInParallel(unittest.TestCase):
    """

    """
    pass


class TestGetCnvCallJob(unittest.TestCase):
    """

    """
    pass


class TestGetDependentFiles(unittest.TestCase):
    """

    """
    pass


class TestGetJobStates(unittest.TestCase):
    """

    """
    pass


class TestGetLaunchedWorkflowIds(unittest.TestCase):
    """

    """
    pass


class TestGetProjects(unittest.TestCase):
    """

    """
    pass


class TestGetXlsxReports(unittest.TestCase):
    """

    """
    pass


class TestGetSingleDir(unittest.TestCase):
    """

    """
    pass


class TestGetLatestDiasBatchApp(unittest.TestCase):
    """

    """
    pass


class TestRunBatch(unittest.TestCase):
    """

    """
    pass


class TestReadGenepanelsFile(unittest.TestCase):
    """

    """
    pass



class TestUploadManifest(unittest.TestCase):
    """

    """
    pass
