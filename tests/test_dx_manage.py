"""Tests for dx_manage"""
import concurrent
import unittest
from unittest.mock import patch

import dxpy
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
    """ """

    pass


class TestCreateFolder(unittest.TestCase):
    """ """

    pass


@patch('bin.utils.dx_manage.dxpy.find_data_objects')
@patch(
    'bin.utils.dx_manage.concurrent.futures.ThreadPoolExecutor.submit',
    wraps=concurrent.futures.ThreadPoolExecutor().submit
)
class TestFindInParallel(unittest.TestCase):
    """
    Tests for dx_manage.find_in_parallel

    Function takes a project and list of search terms for which to
    search for data objects. Searching is done with a ThreadPool of
    32 concurrent threads for speedy searching.
    """

    def test_items_correctly_chunked(self, mock_submit, mock_find):
        """
        Test that when a large number of items are passed in that these
        are correctly chunked into lists of 100 items. This is done
        to generate a regex pattern of up to 100 patterns for a single
        call of dxpy.find_data_objects to reduce the total API calls
        in favour of more 'expensive' calls (i.e. more API server load
        per call).

        To test this, we will generate a list of items of 350 and use
        the number of calls to ThreadPoolExecutor.submit() as a proxy
        to know we have correctly chunked this to 4 lists.
        """
        dx_manage.find_in_parallel(
            project='project-xxx',
            items=[f"sample_{x}" for x in range(350)]
        )

        assert mock_submit.call_count == 4, (
            'items not correctly chunked for concurrent searching'
        )

    def test_results_correctly_returned_as_single_list(self, mock_submit, mock_find):
        """
        Test that when we call the find in parallel that we correctly
        just return a single list of items returned from the find.

        Lazily just return the same for all _find calls and check length
        """
        mock_find.return_value = ['foo', 'bar', 'baz']

        output = dx_manage.find_in_parallel(
            project='project-xxx',
            items=[f"sample_{x}" for x in range(350)]
        )

        # we have 4 concurrent threads => 4 calls, each has a return of
        # length 3 items => expect a single list of 12 items
        with self.subTest("correct length"):
            assert len(output) == 12

        with self.subTest("correct types"):
            # test for correctly flattened to single list
            assert all([type(x) == str for x in output])


    def test_exceptions_caught_and_raised(self, mock_submit, mock_find):
        """
        Test that if one of the searches raises an Exception that this
        is caught and raised
        """
        # raise error one out of 4 of the _find calls
        mock_find.side_effect = [
            ['foo'],
            ['bar'],
            dxpy.exceptions.DXError,  # generic dxpy error
            ['baz']
        ]

        with pytest.raises(dxpy.exceptions.DXError):
            dx_manage.find_in_parallel(
                project='project-xxx',
                items=[f"sample_{x}" for x in range(350)]
            )


    def test_find_input_args(self, mock_submit, mock_find):
        """
        Test that the input arg for the search term to
        dxpy.find_data_objects is as expected with no prefix or suffix
        """
        dx_manage.find_in_parallel(
            project='project-xxx',
            items=[f"sample_{x}" for x in range(5)],

        )

        expected_pattern = "sample_0|sample_1|sample_2|sample_3|sample_4"

        # mocked function args are stored as 2nd item in tuple
        name_arg = mock_find.call_args[1]['name']

        assert name_arg == expected_pattern, "search pattern incorrect"


    def test_find_input_args_with_prefix_suffix(self, mock_submit, mock_find):
        """
        Test that the input arg for the search term to
        dxpy.find_data_objects is as expected with prefix and suffix
        """
        dx_manage.find_in_parallel(
            project='project-xxx',
            items=[f"sample_{x}" for x in range(5)],
            prefix="foo_",
            suffix=".bar"
        )

        expected_pattern = (
            "foo_sample_0.bar|foo_sample_1.bar|foo_sample_2.bar|"
            "foo_sample_3.bar|foo_sample_4.bar"
        )

        # mocked function args are stored as 2nd item in tuple
        name_arg = mock_find.call_args[1]['name']

        assert name_arg == expected_pattern, "search pattern incorrect"


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


class TestReadGenepanelsFile(unittest.TestCase):
    """ """

    pass


class TestUploadManifest(unittest.TestCase):
    """ """

    pass
