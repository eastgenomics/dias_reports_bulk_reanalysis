## Dias Reports Reanalysis

Tool for the rerunning of Dias reports workflow for samples awaiting interpretation.

All specimen IDs in the Clarity export that have a Test Validation Status of "Resulted" and their corresponding booked test codes and date are parsed, these are then searched against all current 002 projects to identify previously run reports to get the instrument IDs and project IDs.

If `--limit`, `--start_date` and / or `--end_date` are specified then the total sample list is limited correspondingly (for `--limit` the oldest samples are retained).

All dependent files for each selected sample to run reports for is then checked for it's archival state, if any are in an archived state this will raise an error unless `--unarchive` is specified, in which case unarchival will be called per project and the script will stop.

Once everything is in a state that reports may be run, it will begin launching eggd_dias_batch jobs for each sequencing runs worth of samples. This will either be in the original 002 projects (if `--testing=False`) or in a given 003 project (if `--testing=True`).

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