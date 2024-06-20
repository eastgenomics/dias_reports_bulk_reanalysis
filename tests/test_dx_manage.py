"""Tests for dx_manage"""
import os
from random import shuffle
import unittest
from unittest.mock import patch

import pytest

from bin.utils import dx_manage

from tests import TEST_DATA_DIR

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


class TestGetSingleDir(unittest.TestCase):
    """ """

    pass


class TestGetLatestDiasBatchApp(unittest.TestCase):
    """ """

    pass


class TestRunBatch(unittest.TestCase):
    """ """

    pass


@patch('bin.utils.dx_manage.dxpy.find_data_objects')
class TestGetLatestGenepanelsFile(unittest.TestCase):
    """
    Tests for dx_manage.read_genepanels_file

    Function searches for genepanels files in 001_Reference/dynamic_files/
    gene_panels/ and returns the details of the latest
    """

    def test_runtime_error_raised_on_finding_no_files(self, mock_find):
        """
        Test that a RuntimeError is correctly raised if no files are
        found in the specified folder
        """
        mock_find.return_value = []

        expected_error = (
            "No genepanels files found in project-Fkb6Gkj433GVVvj73J7x8KbV/"
            "dynamic_files/gene_panels/"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            dx_manage.get_latest_genepanels_file()



    def test_latest_file_selected(self, mock_find):
        """
        Test that the latest file is selected based off the created date
        key in the describe details for each DXFile object
        """
        # file details as returned from dxpy.find_data_objects
        file_details = [
            {
                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                "id": "file-Gkjk6zQ433GyXvqbYGpFBFgx",
                "describe": {
                    "id": "file-Gkjk6zQ433GyXvqbYGpFBFgx",
                    "name": "240610_genepanels.tsv",
                    "created": 1718719358000,
                },
            },
            {
                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                "id": "file-Gj7ygzj42X4ZBqg9068p1fQ4",
                "describe": {
                    "id": "file-Gj7ygzj42X4ZBqg9068p1fQ4",
                    "name": "240405_genepanels.tsv",
                    "created": 1712319487000,
                },
            },
            {
                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                "id": "file-Gj771Q8433GQQZz0gp966kG5",
                "describe": {
                    "id": "file-Gj771Q8433GQQZz0gp966kG5",
                    "name": "240402_genepanels.tsv",
                    "created": 1712222401000,
                },
            },
            {
                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                "id": "file-GgBG75Q433Gk4pY5qpxbgVyz",
                "describe": {
                    "id": "file-GgBG75Q433Gk4pY5qpxbgVyz",
                    "name": "240213_genepanels.tsv",
                    "created": 1708442518000,
                },
            },
        ]

        # shuffle to ensure we don't get it right just from indexing
        shuffle(file_details)

        mock_find.return_value = file_details

        correct_file = {
            "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
            "id": "file-Gkjk6zQ433GyXvqbYGpFBFgx",
            "describe": {
                "id": "file-Gkjk6zQ433GyXvqbYGpFBFgx",
                "name": "240610_genepanels.tsv",
                "created": 1718719358000,
            },
        }

        selected_file = dx_manage.get_latest_genepanels_file()

        assert selected_file == correct_file, (
            'incorrect genepanels file selected'
        )


@patch('bin.utils.dx_manage.dxpy.DXFile')
class TestReadGenepanelsFile(unittest.TestCase):
    """
    Tests for dx_manage.read_genepanels_file

    Function takes in file details returned from
    dx_manage.read_latest_genepanels_file and returns the clinical
    indication and panel name columns as a DataFrame
    """
    # read the contents of the example genepanels we have stored in the
    # test data dir to patch in reading from DNAnexus, call read() to
    # return contents as a string like is done in DXFile.read()
    with open(os.path.join(TEST_DATA_DIR, 'genepanels.tsv')) as fh:
        contents = fh.read()

    def test_contents_correctly_parsed(self, mock_file):
        """
        Test that the contents are correctly parsed
        """
        mock_file.return_value.read.return_value = self.contents

        parsed_genepanels = dx_manage.read_genepanels_file(
            file_details={
                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                "id": "file-Gkjk6zQ433GyXvqbYGpFBFgx",
                "describe": {
                    "id": "file-Gkjk6zQ433GyXvqbYGpFBFgx",
                    "name": "240610_genepanels.tsv",
                    "created": 1718719358000,
                }
            }
        )

        # test some features of the returned dataframe, we expect 2
        # columns `indication` and `panel_name` with 348 rows
        with self.subTest('correct number of rows'):
            assert len(parsed_genepanels.index) == 348

        with self.subTest('correct column names'):
            assert parsed_genepanels.columns.tolist() == [
                'indication', 'panel_name'
            ]

        with self.subTest('correct first row'):
            correct_row = ['C1.1_Inherited Stroke', 'CUH_Inherited Stroke_1.0']

            assert parsed_genepanels.iloc[0].tolist() == correct_row

        with self.subTest('correct last row'):
            correct_row = [
                'R99.1_Common craniosynostosis syndromes_P',
                'Common craniosynostosis syndromes_1.2'
            ]

            assert parsed_genepanels.iloc[-1].tolist() == correct_row

        with self.subTest('total unique indications'):
            assert len(parsed_genepanels['indication'].unique().tolist()) == 280

        with self.subTest('total unique panel names'):
            assert len(parsed_genepanels['panel_name'].unique().tolist()) == 318


class TestUploadManifest(unittest.TestCase):
    """ """

    pass
