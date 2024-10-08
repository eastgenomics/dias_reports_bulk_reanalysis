"""
General utility functions
"""
from collections import defaultdict
import concurrent
from datetime import datetime
import json
from os import path
import re
from typing import List, Union

import dxpy
import pandas as pd


def call_in_parallel(func, items, ignore_missing=False, **kwargs) -> list:
    """
    Calls the given function in parallel using concurrent.futures on
    the given set of items (i.e for calling dxpy.describe() on multiple
    object IDs).

    Additional arguments specified to kwargs are directly passed to the
    specified function.

    Parameters
    ----------
    func : callable
        function to call on each item
    items : list
        list of items to call function on
    ignore_missing : bool
        controls if to just print a warning instead of raising an
        exception on a dxpy.exceptions.ResourceNotFound being raised.
        This is most likely from a file that has been deleted and we are
        just going to default to ignoring these

    Returns
    -------
    list
        list of responses
    """
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        concurrent_jobs = {
            executor.submit(func, item, **kwargs): item for item in items
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                results.append(future.result())
            except Exception as exc:
                if (
                    ignore_missing and
                    isinstance(exc, dxpy.exceptions.ResourceNotFound)
                ):
                    # dx object does not exist and specifying to skip,
                    # just print warning and continue'
                    print(
                        f'WARNING: {concurrent_jobs[future]} could not be '
                        'found, skipping to not raise an exception'
                    )
                    continue

                # catch any other errors that might get raised during querying
                print(
                    f"\nError getting data for {concurrent_jobs[future]}: {exc}"
                )
                raise exc

    return results


def date_str_to_datetime(date) -> int:
    """
    Turn 6 digit date str of yymmdd into datetime object

    Parameters
    ----------
    date : str | int
        date to convert

    Returns
    -------
    datetime
        datetime object of given str | int

    Raises
    ------
    AssertionError
        Raised when incorrect number of
    """
    date = str(date)

    assert re.fullmatch(r'2[0-9](0[0-9]|1[0-2])[0-3][0-9]', date), (
        "Date provided does not seem valid"
    )

    # split parts of date out, removing leading 0 (required for datetime)
    year, month, day = [
        int(date[i : i + 2].lstrip("0")) for i in range(0, len(date), 2)
    ]

    return datetime(year=int(f"20{year}"), month=month, day=day)


def filter_non_unique_specimen_ids(reports) -> Union[list, dict]:
    """
    Filter out any samples that exist in more than one 002 project by
    specimen ID where we can't unambiguously identify them against a
    single instrument ID. This is being done since we only extract the
    specimen ID from Clarity.

    Parameters
    ----------
    reports : list
        list of sample report objects to filter down

    Returns
    -------
    list
        sample reports where specimen ID - instrument ID is unique
    dict
        mapping of specimen ID to report dict where more than one
        instrument ID is present for a specimen ID
    """
    unique = []
    non_unique = defaultdict(list)

    # first map specimen to unique instrument IDs
    sample_map = defaultdict(list)
    for report in reports:
        if report['instrument_id'] not in sample_map[report['specimen_id']]:
            sample_map[report['specimen_id']].append(report['instrument_id'])

    # split out reports where the specimen matches more than one instrument
    for report in reports:
        if len(sample_map[report['specimen_id']]) > 1:
            non_unique[report['specimen_id']].append(report)
        else:
            unique.append(report)

    return unique, non_unique


def filter_clarity_samples_with_no_reports(clarity_samples, samples_w_reports):
    """
    Filter the sample list by the reports returned from DNAnexus to
    highlight which have no reports and won't be run

    Parameters
    ----------
    clarity_samples : dict
        dict of samples specimen IDs to test codes and dates parsed
        from Clarity
    samples_w_reports : list
        list of sample identifiers for those with xlsx reports
    """
    # pre-filter all specimen IDs from reports data
    reports_specimens = [s.get('specimen_id') for s in samples_w_reports]

    clarity_w_reports = {
        specimen: data for specimen, data in clarity_samples.items()
        if specimen in reports_specimens
    }
    clarity_w_out_reports = {
        specimen: data for specimen, data in clarity_samples.items()
        if specimen not in reports_specimens
    }

    print(
        "Total no. of outstanding samples from Clarity with no prior reports "
        f"in DNAnexus: {len(clarity_w_out_reports.keys())}"
    )
    print(
        f"Total samples available to run reports for: "
        f"{len(clarity_w_reports.keys())}"
    )

    # TODO - figure out if we need to do anything return here


def group_samples_by_project(samples, projects) -> dict:
    """
    Group the list of sample reports by the project they are from and
    adds the project name as an additional key.

    Returns as the structure:
    {
        'project-xxx': {
            'project_name': '002_240401_A01295_0334_XXYNSHDBDR',
            'samples': [
                {
                    'sample': '123456-23251R0047',
                    'instrument_id': '123456',
                    'specimen_id': '23251R0047',
                    'codes': ['R134'],
                    'date': datetime(2023, 9, 22, 0, 0)
                }
                ...
            ]
        }
    },
    {
        'project-yyy': {
            ...


    Parameters
    ----------
    samples : list
        list of sample details
    projects : dict
        describe details of all 002 projects

    Returns
    -------
    list
        report object lists split by project
    """
    project_samples = defaultdict(lambda: defaultdict(list))

    for sample in samples:
        project_samples[sample['project']]['samples'].append(sample)
        project_samples[sample['project']]['project_name'] = projects.get(
            sample['project']).get('name')

    print(
        f"{len(samples)} samples present in {len(project_samples.keys())} "
        "DNAnexus projects to run reports for"
    )

    return {k: dict(v) for k, v in dict(project_samples).items()}


def group_dx_objects_by_project(dx_objects) -> dict:
    """
    Groups a list of DNAnexus objects by the project they are in.

    Dict returned contains the project name and items in that project
    under separate keys with the following structure:

    {
        'project-xxx': {
            'project_name': '002_240401_A01295_0334_XXYNSHDBDR',
            'items': [
                {
                    'id': 'file-xxx',
                    'project': 'project-xxx',
                    'describe': {
                        ...
                    }
                }
                ...
            ]
        }
    },
    {
        'project-yyy': {
            ...


    Parameters
    ----------
    dx_objects : list
        list of DNAnexus objects to split

    Returns
    -------
    dict
        DNAnexus objects split by project
    """
    # first get all project names for the project IDs for the given objects
    projects = set([x['project'] for x in dx_objects])
    project_details = call_in_parallel(dxpy.describe, projects)
    project_names = dict(set([(x['id'], x['name']) for x in project_details]))

    project_objects = defaultdict(lambda: defaultdict(list))

    for item in dx_objects:
        name = project_names[item['project']]
        project_objects[item['project']]['project_name'] = name
        project_objects[item['project']]['items'].append(item)

    return project_objects


def add_clarity_data_back_to_samples(samples, clarity_data) -> list:
    """
    Add in the test codes and date as additional keys for each sample

    Parameters
    ----------
    samples : list
        sample data returned from reports jobs
    clarity_data : dict
        mapping of specimen ID to test codes and date from Clarity

    Returns
    -------
    list
        list of sample info with test codes and date added from Clarity

    Raises
    ------
    RuntimeError
        Raised if a specimen ID is not present in the clarity data
    """
    merged_sample_data = []

    for sample in samples:
        clarity_sample = clarity_data.get(sample['specimen_id'])

        if not clarity_sample:
            # this shouldn't happen since we've taken the specimen ID
            # from the sample codes dict to make the project_samples dict
            raise RuntimeError(
                f"Error with sample {sample['sample']} - no test codes for "
                "the specimen ID found in Clarity"
            )

        sample['codes'] = list(set(
            clarity_data.get(sample['specimen_id']).get('codes')
        ))
        sample['date'] = clarity_data.get(sample['specimen_id']).get('date')

        merged_sample_data.append(sample)

    return merged_sample_data


def limit_samples(samples, limit=None, start=None, end=None) -> dict:
    """
    Limits the number of samples retained by integer and / or range of
    dates

    Parameters
    ----------
    samples : list
        list of per sample data
    limit : int
        number of samples to limit by
    start : int
        earliest date of samples in Clarity to restrict running reports for
    end : int
        latest date of samples in Clarity to restrict running reports for

    Returns
    -------
    dict
        limited samples list
    """
    # set date defaults if not specified
    if start:
        start = date_str_to_datetime(start)
    else:
        start = datetime(year=1970, month=1, day=1)

    if not end:
        end = datetime.now().strftime('%y%m%d')

    end = date_str_to_datetime(end)

    # pre-sort sample list by booked in datetime stored against each
    samples = sorted(samples, key=lambda d: d['date'])

    print(
        "\nLimiting samples retained for running reports, currently have "
        f"{len(samples)} samples from Clarity.\n\nEarliest booked sample in "
        f"Clarity export: {samples[0]['date'].strftime('%Y-%m-%d')}\n"
        f"Latest booked sample in Clarity export: "
        f"{samples[-1]['date'].strftime('%Y-%m-%d')}\nLimits specified:\n\t"
        f"Maximum number samples: {limit}\n\tDate range: "
        f"{start.strftime('%Y-%m-%d')} : {end.strftime('%Y-%m-%d')}"
    )

    limited_samples = []
    sample_dates = []
    selected_samples = 0

    for sample in samples:
        if limit:
            if selected_samples >= limit:
                print(f"Hit limit of {limit} samples to retain")
                break

        if not start <= sample['date'] <= end:
            continue

        # sample within date range and not hit limit => select it
        limited_samples.append(sample)
        sample_dates.append(sample['date'])
        selected_samples += 1

    if not limited_samples:
        # no samples left in selected date range => exit
        print(
            "\nWARNING: no samples present in Clarity from the provided "
            "date range. Exiting now."
        )
        exit(0)

    print(
        f"{len(limited_samples)} samples selected. Earliest sample: "
        f"{min(sample_dates).strftime('%Y-%m-%d')}. Latest sample: "
        f"{max(sample_dates).strftime('%Y-%m-%d')}.\n"
    )

    return limited_samples


def filter_reports_with_variants(reports, report_field) -> list:
    """
    Using a list of workflow analysis IDs, returns a list of file IDs of
    xlsx reports with one or more variants in from the xlsx file 'include'
    sheet

    Parameters
    ----------
    reports : list
        list of DXAnalysis objects
    report_field : str
        output field of the xlsx file (i.e for SNV or CNV)

    Returns
    -------
    list
        list of file IDs of reports with variants to download
    """
    # get the xlsx report file IDs to find those containing filtered
    # variants by using the 'included' key in the details metadata,
    # first filtering out jobs with no output
    xlsx_report_ids = [
        job.get('output').get(report_field).get('$dnanexus_link')
        for job in reports
        if job.get('output') and job.get('output').get(report_field)
    ]

    xlsx_details = call_in_parallel(
        dxpy.describe,
        xlsx_report_ids,
        ignore_missing=True,
        fields={'details'},
        default_fields=True
    )

    # get IDs of reports that have filtered variants, details key can
    # either be included or variants because why not so check both
    xlsx_w_variants = [
        x['id'] for x in xlsx_details for field in ['included', 'variants']
        if x['details'].get(field, 0) > 0
    ]

    # get original reports workflows for the above reports to be able
    # to just download both the xlsx and coverage reports for those
    workflows_w_variants = [
        x for x in reports
        if x.get('output') and x.get('output').get(
            report_field, {}).get('$dnanexus_link') in xlsx_w_variants
    ]

    # get the file IDs of our output files to download
    xlsx_ids = [
        x['output'][report_field]['$dnanexus_link'] for x in workflows_w_variants
    ]

    coverage_ids = [
        x['output'].get('stage-rpt_athena.report', {}).get('$dnanexus_link')
        for x in workflows_w_variants
    ]

    # ensure we drop any None values from the mess of selecting
    file_ids = [x for x in xlsx_ids + coverage_ids if x]

    return file_ids


def parse_config() -> Union[dict, dict]:
    """
    Parse config file of manually specified Dias single paths and CNV
    call job IDs.

    These are stored in configs/manually_selected.json, and are required
    where more than one Dias single path / CNV call job is present for
    a given project and cannot be unambiguously selected.

    Returns
    -------
    dict
        mapping of project IDs -> CNV call job IDs
    dict
        mapping of project IDs -> Dias single paths
    """
    config = path.join(
        path.dirname(path.abspath(__file__)),
        "../../configs/manually_selected.json"
    )

    with open(config) as fh:
        contents = json.load(fh)

    return contents.get('cnv_call_jobs'), contents.get('dias_single_paths')


def parse_clarity_export(export_file) -> dict:
    """
    Parse the xlsx export from Clarity to get the samples and test codes

    Parameters
    ----------
    export_file : str
        file name of Clarity export to parse

    Returns
    -------
    dict
        dict mapping specimen ID to test code(s) and booked date
    """
    clarity_df = pd.read_excel(export_file)

    clarity_df['Specimen Identifier'] = clarity_df[
        'Specimen Identifier'].str.replace('SP-', '')

    # remove any cancelled and pending samples
    clarity_df = clarity_df[clarity_df['Test Validation Status'] == 'Resulted']

    clarity_df = clarity_df[
        clarity_df['Test Directory Test Code'] != 'Research Use'
    ]

    clarity_df['Test Directory Test Code'].fillna(value='', inplace=True)

    # turn the date time column into just valid date type
    clarity_df['Received Specimen Date Time'] = pd.to_datetime(
        clarity_df['Received Specimen Date Time']
    ).dt.strftime('%y%m%d')

    # generate mapping of specimen ID to list of test codes and booked date
    sample_code_mapping = {
        x['Specimen Identifier']: {
            'codes': x['Test Directory Test Code'].split('|'),
            'date': date_str_to_datetime(x['Received Specimen Date Time'])
        } for x in clarity_df.to_dict('records')
    }

    return sample_code_mapping


def parse_sample_identifiers(reports) -> list:
    """
    Return the required fields from the xlsx report file name as a dict
    per sample of the project, full samplename, instrument ID and the
    specimen ID

    Parameters
    ----------
    reports : list
        list of file object dicts

    Returns
    -------
    list
        list of dicts with required sample details

    Raises
    ------
    RuntimeError
        Raised if any report names are invalid
    """
    # basic sense check that we don't have anything named like X12345.xlsx
    # that won't pass the below parsing
    invalid = [
        x['describe']['name'] for x in reports if not
        re.match(r'[\w]+-[\w\-]+_[\w\-\.:]+\.xlsx', x['describe']['name'])
    ]

    if invalid:
        raise RuntimeError(
            "ERROR: xlsx reports found that specimen and instrument "
            f"IDs could not be parsed from: {', '.join(invalid)}"
        )


    samples = [
        {
            'project': x['project'],
            'sample': x['describe']['name'].split('_')[0],
            'instrument_id': x['describe']['name'].split('-')[0],
            'specimen_id': x['describe']['name'].split('-')[1]
        } for x in reports
    ]

    # ensure we don't have duplicates from multiple reports jobs
    samples = [dict(s) for s in set(frozenset(d.items()) for d in samples)]

    # sort in some order for consistency of returning and testing
    samples = sorted(samples, key=lambda d: d['sample'])

    return samples


def split_genepanels_test_codes(genepanels) -> pd.DataFrame:
    """
    Split out R/C codes from full CI name for easier matching
    against manifest

    Taken from eggd_dias_batch.utils.split_genepanels_test_codes:
    https://github.com/eastgenomics/eggd_dias_batch/blob/b63a04e2d421a246017e984efcc2a9eef85fbeaf/resources/home/dnanexus/dias_batch/utils/utils.py#L351C1-L405C22

    +-----------------------+--------------------------+
    |      indication      |        panel_name         |
    +-----------------------+--------------------------+
    | C1.1_Inherited Stroke | CUH_Inherited Stroke_1.0 |
    | C2.1_INSR             | CUH_INSR_1.0             |
    +-----------------------+--------------------------+

                                    |
                                    ▼

    +-----------+-----------------------+---------------------------+
    | test_code |      indication      |        panel_name          |
    +-----------+-----------------------+---------------------------+
    | C1.1      | C1.1_Inherited Stroke |  CUH_Inherited Stroke_1.0 |
    | C2.1      | C2.1_INSR             |  CUH_INSR_1.0             |
    +-----------+-----------------------+---------------------------+


    Parameters
    ----------
    genepanels : pd.DataFrame
        dataframe of genepanels with 3 columns

    Returns
    -------
    pd.DataFrame
        genepanels with test code split to separate column

    Raises
    ------
    RuntimeError
        Raised when test code links to more than one clinical indication
    """
    genepanels['test_code'] = genepanels['indication'].apply(
        lambda x: x.split('_')[0] if re.match(r'[RC][\d]+\.[\d]+', x) else x
    )
    genepanels = genepanels[['test_code', 'indication', 'panel_name']]

    # sense check test code only points to one unique indication
    for code in set(genepanels['test_code'].tolist()):
        code_rows = genepanels[genepanels['test_code'] == code]
        if len(set(code_rows['indication'].tolist())) > 1:
            raise RuntimeError(
                f"Test code {code} linked to more than one indication in "
                f"genepanels!\n\t{code_rows['indication'].tolist()}"
            )

    print(f"Genepanels file: \n{genepanels}")

    return genepanels


def validate_test_codes(all_sample_data, genepanels) -> None:
    """
    Parse through manifest dict of sampleID -> test codes to check
    all codes are valid and exclude those that are invalid against
    genepanels file

    Parameters
    ----------
    all_sample_data : list
        list of per sample data, including sample ID, booked test codes
        and booked date
    genepanels : pd.DataFrame
        dataframe of genepanels file

    Returns
    -------
    list
        list of sample data with validated test codes
    dict
        dict of sample ID -> invalid test codes
    """
    print("\n \nChecking test codes in manifest are valid")
    valid = []
    invalid = defaultdict(list)

    genepanels = split_genepanels_test_codes(genepanels)
    genepanels_test_codes = sorted(set(genepanels['test_code'].tolist()))

    print(f"Current valid test codes:\n\t{genepanels_test_codes}")

    for sample_data in all_sample_data:
        sample = sample_data['sample']
        test_codes = sample_data['codes']

        sample_valid_test = []
        sample_invalid_test = []

        if test_codes == []:
            # sample has no booked tests => chuck it in the error bucket
            invalid[sample].append('No tests booked for sample')
            continue

        for test in test_codes:
            if test in genepanels_test_codes or re.search(r'HGNC:[\d]+', test):
                sample_valid_test.append(test)
            elif test.lower().replace(' ', '') == 'researchuse':
                # more Epic weirdness, chuck these out but don't break
                print(
                    f"WARNING: {sample} booked for '{test}' test, "
                    f"skipping this test code and continuing..."
                )
            else:
                sample_invalid_test.append(test)

        if sample_valid_test:
            # one or more valid test(s) for sample, build back the sample
            # data to return with verified valid codes
            sample_data['codes'] = sample_valid_test
            valid.append(sample_data)

        if sample_invalid_test:
            # sample had one or more invalid test code
            invalid[sample].extend(sample_invalid_test)

    if invalid:
        printable_invalid = "\n\t".join(
            f"{k} : {v}" for k, v in invalid.items()
        )
        print(
            "\nWARNING: one or more samples with invalid test code(s)\n"
            "These sample-tests will be excluded for reanalysis:\n\n\t"
            f"{printable_invalid}\n"
        )
    else:
        print("All sample test codes valid!")

    return valid, invalid


def write_manifest(project_name, sample_data, now, ignore_codes) -> List[dict]:
    """
    Write Epic manifest file of all samples for given project

    Parameters
    ----------
    project_name : str
        name of project for naming manifest
    sample_data : list
        list of dicts of sample data (IDs and test code(s))
    now : str
        current datetime for naming
    ignore_codes : list
        list of test codes to ignore and not add to the manifest

    Returns
    -------
    str
        file name of manifest generated
    """
    print(f"\nGenerating manifest data for {len(sample_data)} samples")

    manifest = f"{project_name}-{now}_reanalysis.manifest"
    count = 0

    with open(manifest, "w") as fh:
        fh.write(
            "batch\nInstrument ID;Specimen ID;Re-analysis Instrument ID;"
            "Re-analysis Specimen ID;Test Codes\n"
        )

        for sample in sample_data:
            codes = [x for x in sample['codes'] if x not in ignore_codes]
            for code in codes:
                fh.write(
                    f"{sample['instrument_id']};{sample['specimen_id']}"
                    f";;;{code}\n"
                )
                count += 1

    print(f"{count} sample - test codes written to file {manifest}")

    return manifest


def write_to_log(log_file, key, job_ids) -> None:
    """
    Writes given job IDs as an array to output JSON log file under the
    specified key name

    Parameters
    ----------
    log_file : str
        file name of JSON log to write
    key : str
        name of field to write job IDs to
    job_ids : list
        list of job IDs to write

    Raises
    ------
    AssertionError
        Raised if given log_file is not a json
    """
    assert log_file.endswith('.json'), (
        f'Specified log file {log_file} does not have a .json suffix'
    )

    log_file = path.abspath(path.join(
        path.dirname(path.abspath(__file__)),
        f"../../logs/{log_file}"
    ))

    if path.exists(log_file):
        with open(log_file, 'r') as fh:
            log_data = json.load(fh)
    else:
        log_data = {}

    log_data[key] = job_ids

    with open(log_file, 'w') as fh:
        json.dump(log_data, fh)

    print(f"Launched jobs IDs  for {key} written to {log_file}")


def read_from_log(log_file) -> dict:
    """
    Reads in JSON log file containing launched job IDs
    Parameters
    ----------
    log_file : str
        log file to read job IDs from
    Returns
    -------
    dict
        contents of log file
    Raises
    ------
    AssertionError
        Raised if a non JSON file is provided
    """
    assert log_file.endswith('.json'), 'JSON file not provided to read from'

    with open(log_file) as fh:
        contents = json.loads(fh.read())

    return contents
