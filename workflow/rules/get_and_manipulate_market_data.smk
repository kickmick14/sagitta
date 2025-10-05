""" 
" Snakefile to fetch and clean raw market info, then creating
" indicator information
"
" @author: Michael Kane
" @date:   16/09/2025
"""
import os
from pathlib import Path
from datetime import date


# Get repo REPOROOT path
REPOROOT = os.getenv("SAGITTA_REPO_ROOT")

# Set config file for job
configfile: REPOROOT + "/config/market_config.yaml"

# Market config
pair      = config["client"]["pair"]
period    = config["client"]["period"]
start     = config["client"]["start"]
today     = date.today()
save_name = REPOROOT + f"/data/raw/{pair}_{period}_{start}_{today}"

rule all:
    input:
        save_name + ".csv",
        save_name + ".parquet"

rule get_market_date:
    output:
        save_name + ".csv",
        save_name + ".parquet"
    params:
        donwload_script = REPOROOT + "/workflow/scripts/fetch/download_market_test_data.py",
        keys_path       = REPOROOT + "/config/secrets/keys.json",
        save_name       = save_name,
        pair            = pair,
        period          = period,
        start           = start
    log:
        out = REPOROOT + "/logs/get_and_manipulate_market_data.out",
        err = REPOROOT + "/logs/get_and_manipulate_market_data.err"
    shell:
        r"""
        mkdir -p $SAGITTA_RAW_DATA_DIR &&
        python {params.donwload_script}  \
          --keys_path {params.keys_path} \
          --save_name {params.save_name} \
          --pair      {params.pair}      \
          --period    {params.period}    \
          --start     {params.start}     \
          > {log.out} 2> {log.err}
        """