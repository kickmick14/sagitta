"""
" Read in/out functionality
"
" @author: Michael Kane
" @date:   07/09/2025
"""
from json import JSONDecodeError
from pandas.errors import EmptyDataError
import pandas as pd
import json, os, pyarrow

def load_JSON(
        path
        ):
    """
    Helper to load JSON file
    """

    # Check path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    print(f" (IO) Loading {path}... ", end="")

    # Try to open...
    try:
        with open(path, "r") as f:
            data = json.load(f)
    # Check JSON decodes...
    except JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path}: {e}") from e
    # Check readable...
    except OSError as e:
        raise RuntimeError(f"Could not read {path}: {e}") from e
    # File returned!
    else:
        print("success!")
        return data
    


def load_ParquetToDf(
        path,
        columns=None
        ):
    """
    Helper to load a Parquet file into a DataFrame.
    """

    # Check path exists
    if not os.path.exists( path ):
        raise FileNotFoundError( f"File not found: {path}" )

    print( f" (IO) Loading {path}... ", end="" )

    # Try load parquet into pd.df
    try:
        df = pd.read_parquet( path, columns=columns )
    except (OSError, pyarrow.lib.ArrowInvalid) as e:
        raise ValueError( f"Invalid or unreadable Parquet in {path}: {e}" ) from e
    except EmptyDataError as e:
        raise RuntimeError( f"Parquet file {path} is empty: {e}" ) from e
    else:
        print( "success!" )
        return df


def save_DfToCsv(
        df,
        name,
        index=False
        ):
    """
    Function to save a pandas data frame to .CSV
    """

    # Cautionary check of file name
    if name.endswith(".csv"):
        name = name[:4]

    # Add suffix
    name = name + '.csv'

    print(f" (IO) Saving {name}... ", end="")

    # Try to save .csv
    try:
        df.to_csv(name, index=index)
    # If fails...
    except Exception as e:
        raise RuntimeError(f"Failed to save DataFrame to {name}") from e
    # Else success...
    else:
        print("success!")


def save_DfToParquet(
        df,
        name,
        engine="pyarrow",
        index=False
        ):
    """
    Function to save a pandas data frame to .parquet
    """

    # Cautionary check of file name
    if name.endswith(".parquet"):
        name = name[:8]

    # Add suffix
    name = name + '.parquet'

    print(f" (IO) Saving {name} using {engine}... ", end="")

    # Try to save .csv
    try:
        df.to_parquet( name, engine=engine, index=index )
    # If fails...
    except Exception as e:
        raise RuntimeError(f"Failed to save DataFrame to {name}") from e
    # Else success...
    else:
        print("success!")