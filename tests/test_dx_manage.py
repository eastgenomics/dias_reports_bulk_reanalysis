"""Tests for dx_manage"""
import concurrent
import os
from uuid import uuid4
from random import shuffle

import unittest
from unittest.mock import patch

import dxpy
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
    """
    Tests for dx_manage.get_cnv_call_job

    Function returns the job ID of a CNV call job if specified in the
    pre-selected jobs dict (i.e. in projects where multiple CNV call)
    jobs have been run, else searches the executions in the project
    and returns all job IDs found
    """
    def test_selected_job_returned(self):
        """
        Test that where a job ID for a project has been selected that
        this is returned
        """
        cnv_jobs = dx_manage.get_cnv_call_job(
            project='project-xxx',
            selected_jobs={
                'project-xxx': 'job-xxx',
                'project-yyy': 'job-yyy'
            }
        )

        assert cnv_jobs == ['job-xxx'], 'wrong CNV job returned'

    @patch('bin.utils.dx_manage.dxpy.find_jobs')
    def test_job_ids_returned_from_searching(self, mock_find):
        """
        Test that where jobs are searched for that just the IDs are
        correctly returned
        """
        # return of dxpy.find_jobs as called in get_cnv_call_job()
        mock_find.return_value = [
            {
                "id": "job-aaa"
            },
            {
                "id": "job-bbb"
            }
        ]

        # search where the project ID is not in our selected
        cnv_jobs = dx_manage.get_cnv_call_job(
            project='project-zzz',
            selected_jobs={
                'project-xxx': 'job-xxx',
                'project-yyy': 'job-yyy'
            }
        )

        assert sorted(cnv_jobs) == ['job-aaa', 'job-bbb'], (
            'incorrect job IDs returned'
        )


class TestGetJobStates(unittest.TestCase):
    """
    Tests for dx_manage.get_job_states

    Function calls dxpy.describe in parallel using utils.call_in_parallel
    of a given list of job IDs and returns a mapping of the job ID to its state
    """
    @patch('bin.utils.dx_manage.call_in_parallel')
    def test_correct_states_returned(self, mock_parallel):
        """
        Test that the correct format is returned
        """
        # minimal set of describe objects
        mock_parallel.return_value = [
            {
                "id": "job-xxx",
                "region": "aws:eu-central-1",
                "executable": "eggd_foo_bar",
                "state": "runnable"
            },
            {
                "id": "job-yyy",
                "region": "aws:eu-central-1",
                "executable": "eggd_foo_bar",
                "state": "running"
            },
            {
                "id": "job-zzz",
                "region": "aws:eu-central-1",
                "executable": "eggd_foo_bar",
                "state": "done"
            }
        ]

        returned_states = dx_manage.get_job_states(
            ["job-xxx", "job-yyy", "job-zzz"]
        )

        expected_states = {
            "job-xxx": "runnable",
            "job-yyy": "running",
            "job-zzz": "done"
        }

        assert returned_states == expected_states, "job states incorrectly parsed"


@patch('bin.utils.dx_manage.dxpy.describe')
class TestGetLaunchedWorkflowIds(unittest.TestCase):
    """
    Tests for dx_manage.get_launched_workflow_ids

    Function describes a list of dias batch job IDs and returns all
    analysis jobs launched from them as a single list
    """

    def test_launched_jobs_correctly_returned(self, mock_describe):
        """
        Test that the launched jobs get correctly returned
        """
        # minimal describe return on each job
        mock_describe.side_effect = [
            {
                'id': 'job-xxx',
                'state': 'done',
                'output': {
                    'launched_jobs': 'job-aaa,job-bbb,job-ccc'
                }
            },
            {
                'id': 'job-yyy',
                'state': 'done',
                'output': {
                    'launched_jobs': 'job-ddd,job-eee,job-fff'
                }
            }
        ]

        returned_jobs = dx_manage.get_launched_workflow_ids(
            ['job-xxx', 'job-yyy']
        )

        expected_jobs = [
            'job-aaa', 'job-bbb', 'job-ccc', 'job-ddd', 'job-eee', 'job-fff'
        ]

        assert returned_jobs == expected_jobs, 'launched jobs returned incorrect'


    def test_no_launched_jobs_returns_empty_list(self, mock_decribe):
        """
        Test when there are no batch jobs that the function just returns
        an empty list
        """
        output = dx_manage.get_launched_workflow_ids([])

        assert output == [], 'output incorrect for no input jobs'


    def test_failed_jobs_ignored(self, mock_describe):
        """
        Test that failed jobs dias_batch are excluded correctly
        """
        # minimal describe return on each job
        mock_describe.side_effect = [
            {
                'id': 'job-xxx',
                'state': 'done',
                'output': {
                    'launched_jobs': 'job-aaa,job-bbb,job-ccc'
                }
            },
            {
                'id': 'job-yyy',
                'state': 'failed',
                'output': {}
            }
        ]

        returned_jobs = dx_manage.get_launched_workflow_ids(
            ['job-xxx', 'job-yyy']
        )

        expected_jobs = ['job-aaa', 'job-bbb', 'job-ccc']

        assert returned_jobs == expected_jobs, (
            "wrong job IDs returned with failed dias batch job"
        )


class TestGetProjects(unittest.TestCase):
    """
    Tests for dx_manage.get_projects

    Function searches for a list of projects for given assay and returns
    a dictionary of project ID: describe data
    """

    @patch('bin.utils.dx_manage.dxpy.bindings.search.find_projects')
    def test_projects_correctly_returned(self, mock_find):
        """
        Test the return format is correct when searching for projects
        """
        # minimal return from dxpy.bindings.search.find_projects
        mock_find.return_value = [
            {
                "id": "project-xxx",
                "level": "CONTRIBUTE",
                "describe": {
                    "name": "002_run1_CEN"
                }
            },
            {
                "id": "project-yyy",
                "level": "CONTRIBUTE",
                "describe": {
                    "name": "002_run2_CEN"
                }
            },
            {
                "id": "project-zzz",
                "level": "CONTRIBUTE",
                "describe": {
                    "name": "002_run3_CEN"
                }
            }
        ]

        returned_projects = dx_manage.get_projects("CEN")

        expected_projects = {
            "project-xxx": {"name": "002_run1_CEN"},
            "project-yyy": {"name": "002_run2_CEN"},
            "project-zzz": {"name": "002_run3_CEN"},
        }

        assert expected_projects == returned_projects, (
            "projects incorrectly returned"
        )


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
        Test that for the given list of `n` projects, we call
        find_in_parallel `n` times
        """
        dx_manage.get_xlsx_reports(
            all_samples=self.samples,
            projects=self.projects
        )

        assert mock_parallel.call_count == 3, "not called for all projects"

    @patch('bin.utils.dx_manage.dxpy.find_data_objects')
    def test_correct_search_pattern_generated(self, mock_find):
        """
        Test that the search pattern built in dx_manage.find_in_parallel
        that is provided to dxpy.find_data_objects is as expected
        """
        dx_manage.get_xlsx_reports(
            all_samples=self.samples,
            projects=self.projects
        )

        # mocked function passed arguments are stored as 2nd item in tuple
        built_pattern = mock_find.call_args[1]['name']

        expected_pattern = ".*sample_1.*xlsx|.*sample_2.*xlsx|.*sample_3.*xlsx"

        assert built_pattern == expected_pattern, (
            "Search pattern not as expected"
        )


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


@patch('bin.utils.dx_manage.dxpy.bindings.search.find_apps')
class TestGetLatestDiasBatchApp(unittest.TestCase):
    """
    Tests for dx_manage.get_latest_dias_batch_app

    Function searches for published eggd_dias_batch apps to ensure we
    always run with the latest version
    """

    def test_no_app_found_raises_assertion_error(self, mock_find):
        """
        Test when no eggd_dias_batch app found that we raise an
        AssertionError
        """
        mock_find.return_value = []

        with pytest.raises(
            AssertionError,
            match='No app found for eggd_dias_batch'
        ):
            dx_manage.get_latest_dias_batch_app()


    def test_latest_app_returned(self, mock_find):
        """
        Test where we find multiple published versions of the app that
        we correctly select the latest

        With `all_versions=False` it will return just the app tagged with
        default (i.e. the latest), therefore we are just testing that we
        correctly return the app ID from the response list

        Relevant dxpy docs: http://autodoc.dnanexus.com/bindings/python/current/dxpy_search.html#dxpy.bindings.search.find_global_executables
        """
        mock_find.return_value = [
            {
                'id': 'app-GfG4Bf84QQg40v7Y6zKF34KP'
            }
        ]

        app_id = dx_manage.get_latest_dias_batch_app()

        assert app_id == 'app-GfG4Bf84QQg40v7Y6zKF34KP', (
            'latest app ID incorrectly returned'
        )


@patch('bin.utils.dx_manage.dxpy.DXApp')
class TestRunBatch(unittest.TestCase):
    """
    Tests for dx_manage.run_batch

    Function is the main point that input arguments for eggd_dias_batch
    are built and the app launched. Any additional arguments passed from
    the command line are combined in before running the app.
    """
    def test_cnv_reports_input_correct_from_cnv_job_input(self, mock_app):
        """
        Test that cnv_reports boolean input is correctly set based off
        of having a cnv_job to pass as input (i.e we don't want to try
        rerun CNV calling)
        """
        # example input args for dx_manage.run_batch
        inputs = {
            'project': 'project-xxx',
            'batch_app_id': 'app-xxx',
            'single_path': '/output',
            'manifest': 'file-xxx',
            'name': 'dias_batch',
            'batch_inputs': {},
            'assay': 'test',
            'terminate': False
        }

        with self.subTest('cnv_reports incorrect when cnv_job passed'):
            dx_manage.run_batch(**inputs, cnv_job='job-xxx')
            cnv_reports = mock_app.return_value.run.call_args[1]['app_input']['cnv_reports']

            assert cnv_reports == True

        with self.subTest('cnv_reports incorrect when cnv_job not passed'):
            dx_manage.run_batch(**inputs, cnv_job=None)
            cnv_reports = mock_app.return_value.run.call_args[1]['app_input']['cnv_reports']

            assert cnv_reports == False


    def test_additional_batch_inputs_passed_to_app_inputs(self, mock_app):
        """
        Test that when the optional `batch_inputs` is given to pass
        additional arguments to dias batch from the cmd line args that
        these are passed into the app input
        """
                # example input args for dx_manage.run_batch
        inputs = {
            'project': 'project-xxx',
            'batch_app_id': 'app-xxx',
            'single_path': '/output',
            'manifest': 'file-xxx',
            'name': 'dias_batch',
            'assay': 'test',
            'terminate': False
        }

        dx_manage.run_batch(
            **inputs,
            cnv_job='job-xxx',
            batch_inputs={'split_tests': False}
        )
        app_input = mock_app.return_value.run.call_args[1]['app_input']

        assert app_input.get('split_tests') == False, (
            'additional batch inputs not correctly in passed app inputs'
        )


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


@patch('bin.utils.dx_manage.dxpy.upload_local_file')
class TestUploadManifest(unittest.TestCase):
    """
    Tests for dx_manage.upload_manifest

    Function calls dxpy.upload_local_file to perform the upload, deletes
    the local file and then returns the uploaded file ID
    """

    def test_local_file_removed(self, mock_upload):
        """
        Test that the local file is removed after calling upload
        """
        # generate random file
        test_file = os.path.join(TEST_DATA_DIR, uuid4().hex)

        open(test_file, 'w').close()

        dx_manage.upload_manifest(
            manifest=test_file,
            path='/'
        )

        assert not os.path.exists(test_file), 'local file not deleted'


    @patch('bin.utils.dx_manage.os.remove')
    def test_id_returned_from_dxfile_object(self, mock_remove, mock_upload):
        """
        Test that the uploaded file ID is correctly returned from the
        DXFile object
        """
        # example DXFile object response from uploading file, dxid required
        # to be a random 24 character alphanumeric string to setup object
        mock_upload.return_value = dxpy.bindings.dxfile.DXFile(
            dxid='file-GgQP6X84bjX3J53Vv1Yxyz7b'
        )

        file_id = dx_manage.upload_manifest(
            manifest='',
            path='/'
        )

        assert file_id == 'file-GgQP6X84bjX3J53Vv1Yxyz7b', (
            "uploaded file ID incorrect"
        )


