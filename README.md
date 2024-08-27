## Dias Reports Reanalysis

![pytest](https://github.com/eastgenomics/dias_reports_bulk_reanalysis/actions/workflows/pytest.yml/badge.svg)

Tool for the rerunning of Dias reports workflow for samples awaiting interpretation.

All specimen IDs in the Clarity export that have a Test Validation Status of "Resulted" and their corresponding booked test codes and date are parsed, these are then searched against all current 002 projects to identify previously run reports to get the instrument IDs and project IDs.

If `--limit`, `--start_date` and / or `--end_date` are specified then the total sample list is limited correspondingly (for `--limit` the oldest samples are retained).

All dependent files for each selected sample to run reports for are then checked for their archival state, if any are in an archived state this will raise an error unless `--unarchive` is specified, in which case unarchival will be called per project and the script will stop.

Once everything is in a state that reports may be run, the script may be run to begin launching eggd_dias_batch jobs for each sequencing runs worth of samples. This will either be in the original 002 projects (default) or in a given 003 project (if `--test_project={project-xxx}`).

Once jobs are launched, the state of these jobs will be monitored and printed to stdout if `--monitor` is specified. Job IDs of launched dias_batch, dias reports and eggd_artemis will be written to a JSON log file in the current working directory. This may then be used as input to the download mode to download all output reports.

### Downloading outputs

Once reanalysis has been run and all jobs completed, the output reports may be downloaded by running in download mode, and providing the JSON log file with launched job IDs in as input. This will group up all launched jobs by project, and download the xlsx reports, coverage reports, eggd_artemis file and input multiQC file (if available) to run specific directories. Any reports with no variants in the include tab will be skipped and not downloaded. If any jobs are still in progress this will print a warning and exit without downloading. If any jobs have failed, this will print a warning for those jobs and downloading for successful jobs will continue.

### Usage

#### Locally
- To run reanalysis:
```
python3 bin/run_reports.py reanalysis --assay CEN --clarity_export <export.xlsx>
```

To download output reports:
```
python3 bin/run_reports.py download --job_log launched_jobs_240801_log.json --path output/
```

#### Docker

Building the Docker image from the included Docker file may be done with:
```
docker built -t <image_name>:<image_tag>
```

Running from the built Docker image requires mounting both the `log/` directory and clarity export file for reanalysis, and `output/` directory for downloading output reports, from the container to the host. This is so that when the reanalysis / download completes the log / output files are available outside of the container.

For running reanalysis, it requires confirming if to run all jobs from a user prompt. Therefore this requires first opening a shell interactively in the container, then running the reanalysis command to be able to confirm launching jobs (see example below). Once reanalysis has been launched the container may be exited by simply running `exit`. This is not required for downloading as this has no user prompt.

In addition, access to DNAnexus is required. One way to achieve this is to export the current dx security context as environment variables into the Docker container. This can be done with the following line: `--env-file <(dx env --bash | sed -e "s/export//g" -e "s/'//g")`. This gets the current set security context from the host using `dx env` and formats it as required for passing as an environment 'file' for Docker.

Example command for running reanalysis:
```
# get interactive shell in the container
$ docker run \
    --env-file <(dx env --bash | sed -e "s/export//g" -e "s/'//g") \
    -v $(pwd)/<clarity_export_xlsx>:/reanalysis/clarity_export.xlsx \
    -v $(pwd):/reanalysis/logs \
    <image_name>:<image_tag>

# run reanalysis as would be done locally
python3 bin/run_reports.py reanalysis \
    --clarity_export clarity_export.xlsx \
    --assay CEN \
    --monitor
```
* the working directory in the image is set to `/reanalysis` which contains `bin/` and `logs/` etc
* the clarity export xlsx file `<clarity_export.xlsx>` needs mounting into the container
* the directory for output log file needs mounting in to the container to retain the log file with launched batch IDs for downloading later


Example command for downloading outputs once jobs complete:
```
docker run \
    --env-file <(dx env --bash | sed -e "s/export//g" -e "s/'//g")
    -v $(pwd):/reanalysis/logs \
    -v <local_output_path>:/reanalysis/output
    <image_name>:<image_tag> python3 bin/run_reports.py download \
        --job_log logs/<log_file_from_reanalysis> \
        --path output/
```
* the same log file as specified for mounting in the log file should be specified again
* the log file output with launched jobs should be passed from the `logs/` directory if mounted as above
* `<local_output_path>` should be set for specifying where to download reports outside of the container


### Inputs

Reanalysis inputs:
* `-a` / `--assay`: assay for which to run reports for
* `--clarity_export`: path to file containing export from Clarity
* `--config` (optional): file ID of assay config file to use for eggd_dias_batch, if not specified will use latest in 001_Reference
* `--batch_inputs` (optional): JSON formatted string of additional arguments to pass to eggd_dias_batch
* `--limit` (optional): number of samples to limit running jobs for, if no date range is specified this will default to being the oldest n samples
* `--start_date` (optional): Earliest date to select samples from Clarity to run reports for, to be specified as YYMMDD
* `--end_date` (optional): Latest date to select samples from Clarity to run reports for, to be specified as YYMMDD
* `--unarchive` (optional): controls if to start unarchiving of any required files
* `--test_project` (optional): DNAnexus project ID in which to launch dias batch, if not specified will launch in original 002 projects
* `--terminate` (optional): Controls if to terminate all analysis jobs dias batch launched
* `--monitor` (optional): Controls if to monitor and report on state of launched dias batch jobs
* `--strip_test_codes` (optional): DNAnexus file ID of file containing test codes to be ignored and not added to the manifest, if present in clarity extract.


Download inputs:
* `--job_log`: json log file output from running reanalysis
* `--path`: parent directory in which to download sub directories per run of output reports


## Logging

When jobs have been launched a json log file is generated with the name format `launched_jobs_{yymmdd_hhmm}_log.json`. This stores the job IDs of the launched dias_batch and dias_reports_workflows jobs, allowing for querying the state and accessing output files.

The log file is structured as follows:
```
{
    "dias_batch": [
        "job-Gp3vXPQ4BgGVFxJ76VG89ZbG",
        "job-Gp8Y5Z84ZB47zXq3JJG3qvB0",
        "job-GpFk6pj4z9pgpy7pKGx44KP3",
        "job-GpP3kjj49kjyFG16JPJ59jBV"
    ],
    "dias_reports": [
        "analysis-GpP4gy849kjxGY2YGy7117B2",
        "analysis-GpP4gy049kjfgJ290YPG803B",
        "analysis-GpP4gxQ49kjfgJ290YPG8030",
        "analysis-GpP4gx849kjX9bjk9XjF6K8q"
    ],
    "eggd_artemis": [
        "job-GpFp6v84z9pYz690YgkP7JJX"
    ]
}
```

For any samples with invalid test code(s), these will print a warning to stdout during running and also be written to a JSON log file to review after (named as `{yymmdd_hhmm}_invalid_test_codes.json`).

Both files will always be output to the `logs/` directory.

### Example useful commands to query the log file:

* check the state of dias_batch jobs:
```
$ jq -r '.dias_batch[]' launched_jobs_240729_1035_log.json | xargs -P16 -I{} sh -c "dx describe --json {} | jq -r '[.id,.state] | @tsv'"
job-GpP3kjj49kjyFG16JPJ59jBV    done
job-Gp8Y5Z84ZB47zXq3JJG3qvB0    done
job-Gp3vXPQ4BgGVFxJ76VG89ZbG    done
job-GpFk6pj4z9pgpy7pKGx44KP3    done
```

* check the state of all jobs launched by the dias batch jobs in the log file:
```
$ xargs -P32 -n1 -I{} bash -c 'dx describe --json {} | jq -r "[.id,.state] | @tsv"' <<< $(jq -r '.dias_batch[]' launched_jobs_240808_1311_log.json | xargs -P32 -I{} dx describe --json {} | jq -r '.output.launched_jobs' | sed 's/,/\n/g') | sort -k2
analysis-GppFKxj45jXXZQJYP5y39YqV       done
analysis-GppFKy845jXyJpJXPzq06YYz       done
analysis-GppFKyQ45jXQJQyy3KZKZ0QP       done
analysis-GppFKz045jXyJpJXPzq06YZP       done
analysis-GppFP0045jXYZPg13B3B0fZz       done
analysis-GppFP0845jXy2qx9qv109FY7       done
analysis-GppFP0j45jXyJpJXPzq06Yb6       done
analysis-GppFP1045jXbyF1K93PJKkQG       done
analysis-GppFP1045jXx3ZFz0389KXq2       done
analysis-GppFP1845jXzYP89kqx0gXbg       done
analysis-GppFP1j45jXVbpQf5G3BZ4fF       done
analysis-GppFP2045jXy6XB5ZGF168jG       done
analysis-GppFP2Q45jXVbpQf5G3BZ4fg       done
analysis-GppFP3045jXzYP89kqx0gXf9       done
analysis-GppFP3j45jXV62JYkGg3j03J       done
analysis-GppFP4845jXbyF1K93PJKkVY       done
analysis-GppFP4Q45jXzYP89kqx0gXkP       done
analysis-GppFP5045jXV62JYkGg3j03z       done
analysis-GppFP5845jXbyF1K93PJKkX6       done
analysis-GppFP5j45jXzYP89kqx0gXkv       done
job-GppFP1Q45jXXZQJYP5y39Yvj    failed
job-GppFP6045jXgg95vvYkJqGJ5    failed
```