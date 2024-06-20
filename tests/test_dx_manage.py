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
    """ """

    pass


@patch('bin.utils.dx_manage.dxpy.find_data_objects')
class TestGetSingleDir(unittest.TestCase):
    """
    Tests for dx_manage.get_single_dir

    Function to find the dias single output directory. This achieved by
    using the multiQC html as a proxy for the single dir, as this is
    always generated from the Dias single workflow once.
    """
    def test_manually_selected_path_correctly_used(self, mock_find):
        """
        Test that when a project has a manually selected Dias single
        path in the config (i.e. from having multiple in the project)
        that this is correctly returned
        """
        returned_paths = dx_manage.get_single_dir(
            project='project-xxx',
            selected_paths={
                'project-xxx': '/output/240620/',
                'project-yyy': '/output/240624'
            }
        )

        assert returned_paths == ['project-xxx:/output/240620/'], (
            'manually selected path incorrect'
        )


    def test_single_found_path_correctly_returned_from(self, mock_find):
        """
        Test that when we find a multiQC report that the single path is
        correctly parsed from the describe response
        """
        mock_find.return_value = [
            {
                "project": "project-zzz",
                "id": "file-yyy",
                "describe": {
                    "name": "240229_A01295_0328_BHYG25DRX3_multiqc.html",
                    "folder": "/output/CEN-240302_1503/eggd_MultiQC/"
                }
            }
        ]

        returned_paths = dx_manage.get_single_dir(
            project='project-zzz',
            selected_paths={
                'project-xxx': '/output/240620/',
                'project-yyy': '/output/240624'
            }
        )

        assert returned_paths == ['project-zzz:/output/CEN-240302_1503'], (
            'Found path incorrectly returned'
        )


    def test_no_single_path_does_not_raise_error(self, mock_find):
        """
        Test that if no path is found that no error is raised and an
        empty list is returned, since we will handle the empty path
        upstream of this function
        """
        mock_find.return_value = []

        returned_paths = dx_manage.get_single_dir(
            project='project-zzz',
            selected_paths={
                'project-xxx': '/output/240620/',
                'project-yyy': '/output/240624'
            }
        )

        assert returned_paths == [], 'Missing Dias single dir incorrect'


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
