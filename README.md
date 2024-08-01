## Dias Reports Reanalysis

![pytest](https://github.com/eastgenomics/dias_reports_bulk_reanalysis/actions/workflows/pytest.yml/badge.svg)

Tool for the rerunning of Dias reports workflow for samples awaiting interpretation.

All specimen IDs in the Clarity export that have a Test Validation Status of "Resulted" and their corresponding booked test codes and date are parsed, these are then searched against all current 002 projects to identify previously run reports to get the instrument IDs and project IDs.

If `--limit`, `--start_date` and / or `--end_date` are specified then the total sample list is limited correspondingly (for `--limit` the oldest samples are retained).

All dependent files for each selected sample to run reports for are then checked for their archival state, if any are in an archived state this will raise an error unless `--unarchive` is specified, in which case unarchival will be called per project and the script will stop.

Once everything is in a state that reports may be run, the script may be run to begin launching eggd_dias_batch jobs for each sequencing runs worth of samples. This will either be in the original 002 projects (if `--testing=False`) or in a given 003 project (if `--testing=True`).

Once jobs are launched, the state of these jobs will be monitored and printed to stdout if `--monitor` is specified.

### Usage

```
python3 bin/run_reports.py --assay CEN --clarity_export <export.xlsx>
```


### Inputs

* `-a` / `--assay`: assay for which to run reports for
* `--clarity_export`: path to file containing export from Clarity
* `--config` (optional): file ID of assay config file to use for eggd_dias_batch, if not specified will use latest in 001_Reference
* `--batch_inputs` (optional): JSON formatted string of additional arguments to pass to eggd_dias_batch
* `--limit` (optional): number of samples to limit running jobs for, if no date range is specified this will default to being the oldest n samples
* `--start_date` (optional): Earliest date to select samples from Clarity to run reports for, to be specified as YYMMDD
* `--end_date` (optional): Latest date to select samples from Clarity to run reports for, to be specified as YYMMDD
* `--unarchive` (optional): controls if to start unarchiving of any required files
* `--testing` (optional): Controls where dias batch is run, when testing launch all in one 003 project
* `--terminate` (optional): Controls if to terminate all analysis jobs dias batch launches
* `--monitor` (optional): Controls if to monitor and report on state of launched dias batch jobs

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

### Example useful commands to query the log file:

* check the state of dias_batch jobs:
```
$ jq -r '.dias_batch[]' launched_jobs_240729_1035_log.json | xargs -P16 -I{} sh -c "dx describe --json {} | jq -r '[.id,.state] | @tsv'"
job-GpP3kjj49kjyFG16JPJ59jBV    done
job-Gp8Y5Z84ZB47zXq3JJG3qvB0    done
job-Gp3vXPQ4BgGVFxJ76VG89ZbG    done
job-GpFk6pj4z9pgpy7pKGx44KP3    done
```

* check the state of dias_reports jobs:
```
$ jq -r '.dias_reports[]' launched_jobs_240729_1035_log.json | xargs -P16 -I{} sh -c "dx describe --json {} | jq -r '[.id,.state] | @tsv'"
analysis-GpP4gy049kjfgJ290YPG803B       done
analysis-GpP4gxQ49kjfgJ290YPG8030       done
analysis-GpP4gx849kjX9bjk9XjF6K8q       done
analysis-GpP4gy849kjxGY2YGy7117B2       done
```

* check the state of eggd_artemis jobs:
```
$ jq -r '.eggd_artemis[]' launched_jobs_240729_1035_log.json | xargs -P16 -I{} sh -c "dx describe --json {} | jq -r '[.id,.state] | @tsv'"
job-GpFp6v84z9pYz690YgkP7JJX    done
```
