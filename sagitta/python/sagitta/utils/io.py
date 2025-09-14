"""
" Read in/out functionality
"
" @author: Michael Kane
" @date:   07/09/2025
"""
import json, os
from json import JSONDecodeError

def loadJSON(
        path
        ):
    """
    Helper to load JSON file
    """

    # Check path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    print(f" (IO) Loading {path}...", end="")

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
        print(" success!")
        return data


def dfToCsv(
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

    print(f" (IO) Saving {name}...", end="")

    # Try to save .csv
    try:
        df.to_csv(name, index=index)
    # If fails...
    except Exception as e:
        raise RuntimeError(f"Failed to save DataFrame to {name}") from e
    # Else success...
    else:
        print(" success!")


def dfToParquet(
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

    print(f" (IO) Saving {name} using {engine}...", end="")

    # Try to save .csv
    try:
        df.to_parquet( name, engine=engine, index=index )
    # If fails...
    except Exception as e:
        raise RuntimeError(f"Failed to save DataFrame to {name}") from e
    # Else success...
    else:
        print(" success!")