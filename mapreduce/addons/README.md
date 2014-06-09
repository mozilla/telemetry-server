Telemetry map/reduce to analyze bootstrap add-on performance probes
===================================================================

Files:

- am_exceptions.py: Telemetry map/reduce to crunch raw data into lines describing:
    - exceptions caught during add-on manager initialization
    - histogram of time taken by add-on file scans and bootstrap methods

- combine.py: merge outputs from am_exceptions.py and generate .csv format summaries
    - weekly-addons-{date}.csv.gz
    - weekly-exceptions-{date}.csv.ga

- run.sh: driver script for Telemetry scheduled daily job - downloads actual M/R code
  from Github and executes job

- processExceptions.py: analysis script that runs the telemetry M/R job using am_exceptions.py
  and then produces the output files by gathering the week's data from S3 and running combine.py

- filter_template.json: template for M/R job filter; processExceptions.py creates a copy
  with the desired date for each M/R run
