## Dias Reports Reanalysis

Tool for the rerunning of Dias reports workflow for samples awaiting interpretation

### Usage



### Inputs

* `-a` / `--assay`: assay for which to run reports for
* `--clarity_export`: path to file containing export from Clarity
* `--config` (optional): file ID of assay config file to use for eggd_dias_batch, if not specified will use latest in 001_Reference
* `--batch_inputs` (optional): JSON formatted string of additional arguments to pass to eggd_dias_batch