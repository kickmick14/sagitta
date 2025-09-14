from pathlib import Path
from datetime import date
import yaml

# Get repo root path
ROOT = ( Path(workflow.basedir).resolve() ).parents[1]

# Set config file for job
configfile: str( ROOT / "config/market_config.yaml" )

# Get config
keys_path = str( ROOT / config['keys_path'] )
out_dir   = str( ROOT / config['out_dir'] )
pair      = config['pair']
period    = config['period']
start     = config['start']

# Set outname using date
today     = date.today()

# Set savename for raw market data
save_name = f'{out_dir}/{pair}_{period}_{start}_{today}'


rule all:
    input:
        save_name + '.csv',
        save_name + '.parquet'

rule get_market_date:
    output:
        save_name + '.csv',
        save_name + '.parquet'
    params:
        script_loc = str( ROOT / 'workflow/scripts/fetch/download_market_data.py' ),
        keys_path  = keys_path,
        out_dir    = out_dir,
        save_name  = save_name,
        pair       = pair,
        period     = period,
        start      = start
    log:
        out        = str( ROOT / 'logs/get_and_manipulate_market_data.out' ),
        err        = str( ROOT / 'logs/get_and_manipulate_market_data.err')
    shell:
        r"""
        mkdir -p {params.out_dir} &&
        python {params.script_loc} \
          --keys_path {params.keys_path} \
          --save_name {params.save_name} \
          --pair      {params.pair}      \
          --period    {params.period}    \
          --start     {params.start}     \
          > {log.out} 2> {log.err}
        """