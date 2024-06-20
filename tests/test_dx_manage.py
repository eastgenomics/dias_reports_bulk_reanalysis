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
            "describe": {"archivalState": "live"},
        },
        {
            "project": "project-xxx",
            "id": "file-bbb",
            "describe": {"archivalState": "live"},
        },
        {
            "project": "project-xxx",
            "id": "file-ccc",
            "describe": {"archivalState": "unarchiving"},
        },
        {
            "project": "project-xxx",
            "id": "file-aaa",
            "describe": {"archivalState": "archived"},
        },
        {
            "project": "project-xxx",
            "id": "file-aaa",
            "describe": {"archivalState": "archived"},
        },
    ]

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys

    @patch("bin.utils.dx_manage.find_in_parallel")
    def test_all_states_mixed_returned_correctly(self, mock_find):
        """
        Test that we correctly return the given files by their states
        """
        mock_find.return_value = self.returned_file_details

        live, unarchiving, archived = dx_manage.check_archival_state(
            project="project-xxx",
            sample_data={
                "samples": [{"sample": "sample1"}, {"sample": "sample2"}]
            },
        )

        expected_live = [
            {
                "project": "project-xxx",
                "id": "file-aaa",
                "describe": {"archivalState": "live"},
            },
            {
                "project": "project-xxx",
                "id": "file-bbb",
                "describe": {"archivalState": "live"},
            },
        ]

        expected_unarchiving = [
            {
                "project": "project-xxx",
                "id": "file-ccc",
                "describe": {"archivalState": "unarchiving"},
            }
        ]

        expected_archived = [
            {
                "project": "project-xxx",
                "id": "file-aaa",
                "describe": {"archivalState": "archived"},
            },
            {
                "project": "project-xxx",
                "id": "file-aaa",
                "describe": {"archivalState": "archived"},
            },
        ]

        with self.subTest("live files wrongly identified"):
            assert live == expected_live

        with self.subTest("unarchiving files wrongly identified"):
            assert unarchiving == expected_unarchiving

        with self.subTest("archived files wrongly identified"):
            assert archived == expected_archived

    @patch("bin.utils.dx_manage.find_in_parallel")
    def test_correct_number_files_searched_for(self, mock_find):
        """
        When searching in DNAnexus, there are a set number of patterns
        defined in the function that are searched for each sample provided
        in the sample data.

        We will use the number of files printed to stdout as a proxy for
        the correct list of patterns being built since we don't return them.
        """
        dx_manage.check_archival_state(
            project="project-xxx",
            sample_data={
                "samples": [{"sample": "sample1"}, {"sample": "sample2"}]
            },
        )

        # since we pass 2 samples, we expect 2 * 8 patterns plus the
        # run level excluded intervals bed => 17 file patterns
        stdout = self.capsys.readouterr().out

        expected_stdout = "17 sample files to search for"

        assert (
            expected_stdout in stdout
        ), "Wrong no. files identified to check archival state of"


class TestUnarchiveFiles(unittest.TestCase):
    """
    Tests for dx_manage.unarchive_files()

    Function called by dx_manage.check_archival_state where one or more
    archived files found and unarchive=True set, will go through the
    given file IDs and start the unarchiving process
    """

    # minimal dxpy.find_data_objects() return that we expect to unarchive
    files = {
        "project-xxx": [
            {
                "project": "project-xxx",
                "id": "file-xxx",
                "describe": {
                    "name": "sample1-file1",
                    "archivalState": "archived",
                },
            },
            {
                "project": "project-xxx",
                "id": "file-yyy",
                "describe": {
                    "name": "sample2-file1",
                    "archivalState": "archived",
                },
            },
        ]
    }

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys

    @patch("bin.utils.dx_manage.dxpy.api.project_unarchive")
    @patch("bin.utils.dx_manage.exit")
    def test_unarchiving_called(self, exit, mock_unarchive):
        """
        Test that dxpy.api.project_unarchive() gets called on
        the provided list of DXFile objects
        """
        dx_manage.unarchive_files(self.files)

        mock_unarchive.assert_called()

    @patch("bin.utils.dx_manage.dxpy.api.project_unarchive")
    @patch("bin.utils.dx_manage.exit")
    def test_unarchive_called_per_project(self, exit, mock_unarchive):
        """
        If files found are in more than one project the function
        will loop over each set of files per project, test that this
        correctly happens where files are in 3 projects
        """
        # minimal example of 3 files in 3 separate projects
        files = {
            "project-xxx": [
                {
                    "project": "project-xxx",
                    "id": "file-xxx",
                    "describe": {
                        "name": "sample1-file1",
                        "archivalState": "archived",
                    },
                }
            ],
            "project-yyy": [
                {
                    "project": "project-yyy",
                    "id": "file-yyy",
                    "describe": {
                        "name": "sample2-file1",
                        "archivalState": "archived",
                    },
                }
            ],
            "project-zzz": [
                {
                    "project": "project-zzz",
                    "id": "file-zzz",
                    "describe": {
                        "name": "sample2-file1",
                        "archivalState": "archived",
                    },
                }
            ],
        }

        dx_manage.unarchive_files(files)

        self.assertEqual(mock_unarchive.call_count, 3)

    @patch(
        "bin.utils.dx_manage.dxpy.api.project_unarchive",
        side_effect=Exception("someDNAnexusAPIError"),
    )
    def test_error_raised_if_unable_to_unarchive(self, mock_unarchive):
        """
        If any error is raised during calling dxpy.api.project_unarchive
        it will be caught and raise a RuntimeError, test that if an
        Exception is raised we get the expected error message
        """
        with pytest.raises(RuntimeError, match="Error unarchiving files"):
            dx_manage.unarchive_files(self.files)

    @patch("bin.utils.dx_manage.dxpy.api.project_unarchive")
    @patch("bin.utils.dx_manage.exit")
    def test_check_state_command_correct(self, exit, mock_unarchive):
        """
        Test that when the function calls all the unarchiving, that
        the message printed to stdout with a command to check the state
        of all files is correct
        """
        dx_manage.unarchive_files(self.files)

        expected_stdout = (
            "echo file-xxx file-yyy | xargs -n1 -d' ' -P32 -I{} bash -c 'dx "
            "describe --json {} ' | grep archival | uniq -c"
        )

        assert (
            expected_stdout in self.capsys.readouterr().out
        ), "check state command not as expected"


class TestCreateFolder(unittest.TestCase):
    """ """

    pass


class TestFindInParallel(unittest.TestCase):
    """ """

    pass


class TestGetCnvCallJob(unittest.TestCase):
    """ """

    pass


class TestGetDependentFiles(unittest.TestCase):
    """ """

    pass


class TestGetJobStates(unittest.TestCase):
    """ """

    pass


class TestGetLaunchedWorkflowIds(unittest.TestCase):
    """ """

    pass


class TestGetProjects(unittest.TestCase):
    """ """

    pass


class TestGetXlsxReports(unittest.TestCase):
    """
    Tests for dx_manage.get_xlsx_reports

    Function searches through a list of projects for a list of sample
    names to find all xlsx reports and returns a single list of all
    describe objects for each
    """
    projects = ['project_1', 'project_2', 'project_3']
    samples = ['sample_1', 'sample_2', 'sample_3']

    @patch('bin.utils.dx_manage.find_in_parallel')
    def test_all_projects_searched(self, mock_parallel):
        """
        Test that for the given list of n projects, we call
        find_in_paralle in times
        """
        dx_manage.get_xlsx_reports(
            all_samples=self.samples,
            projects=self.projects
        )

        assert mock_parallel.call_count == 3, "not called for all projects"

    #TODO - come back to this after tests for find_in_parallel fixed
    # @patch('bin.utils.dx_manage.dxpy.find_data_objects')
    # def test_correct_search_pattern_generated(self, mock_find):
    #     """
    #     Test that the search pattern built in dx_manage.find_in_parallel
    #     that is provided to dxpy.find_data_objects is as expected
    #     """
    #     dx_manage.get_xlsx_reports(
    #         all_samples=self.samples,
    #         projects=self.projects
    #     )

    #     # mocked function args are stored as 2nd item in tuple
    #     print(mock_find.call_args[1]['name'])

    #     exit(1)

    @patch('bin.utils.dx_manage.find_in_parallel')
    def test_non_sample_xlsx_correctly_filtered_out(self, mock_parallel):
        """
        Test that when a non sample xlsx is returned (i.e. run level
        eggd_artemis file), that these are filtered out
        """
        # mock returning one sample from each report plus additional
        # run file that should be filtered
        mock_parallel.side_effect = [
            [
                {
                    "id": "file-aaa",
                    "describe": {
                        "name": "sample1.xlsx"
                    }
                }
            ],
            [
                {
                    "id": "file-bbb",
                    "describe": {
                        "name": "sample2.xlsx"
                    }
                }
            ],
            [
                {
                    "id": "file-ccc",
                    "describe": {
                        "name": "sample3.xlsx"
                    }
                },
                {
                    "id": "file-ddd",
                    "describe": {
                        "name": "240229_A01295_0328_BHYG25DRX3_240620.xlsx"
                    }
                }
            ]
        ]

        returned_reports = dx_manage.get_xlsx_reports(
            all_samples=self.samples,
            projects=self.projects
        )

        expected_reports = [
            {
                "id": "file-aaa",
                "describe": {
                    "name": "sample1.xlsx"
                }
            },
            {
                "id": "file-bbb",
                "describe": {
                    "name": "sample2.xlsx"
                }
            },
            {
                "id": "file-ccc",
                "describe": {
                    "name": "sample3.xlsx"
                }
            }

        ]

        assert returned_reports == expected_reports, (
            "run level report not correctly filtered out"
        )


class TestGetSingleDir(unittest.TestCase):
    """ """

    pass


class TestGetLatestDiasBatchApp(unittest.TestCase):
    """ """

    pass


class TestRunBatch(unittest.TestCase):
    """ """

    pass


class TestReadGenepanelsFile(unittest.TestCase):
    """ """

    pass


class TestUploadManifest(unittest.TestCase):
    """ """

    pass
