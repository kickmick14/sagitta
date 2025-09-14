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
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    # Check JSON decodes...
    except JSONDecodeError as e:
        print(" failed!")
        raise ValueError(f"Invalid JSON in {path}: {e}") from e
    # Check readable...
    except OSError as e:
        print(" failed!")
        raise RuntimeError(f"Could not read {path}: {e}") from e
    # File returned!
    else:
        print(" success!")
        return data


def dfToCsv(
        df,
        path,
        index=False
        ):
    """
    Function to save a pandas data frame to .CSV
    """

    # Cautionary check of file name
    if path.endswith(".csv"):
        path = path[:4]

    # Add suffix
    path = path + ".csv"

    print(f" (IO) Saving {path}...", end="")

    # Try to save .csv
    try:
        df.to_csv(path, index=index)
    # If fails...
    except Exception as e:
        print(" failed!")
        raise RuntimeError(f"Failed to save DataFrame to {path}") from e
    # Else success...
    else:
        print(" success!")


def dfToParquet(
        df,
        path,
        engine="pyarrow",
        index=False
        ):
    """
    Function to save a pandas data frame to .parquet
    """

    # Cautionary check of file name
    if path.endswith(".parquet"):
        path = path[:8]

    # Add suffix
    path = path + ".parquet"

    print(f" (IO) Saving {path} using {engine}...", end="")

    # Try to save .csv
    try:
        df.to_parquet( path, engine=engine, index=index )
    # If fails...
    except Exception as e:
        print(" failed!")
        raise RuntimeError(f"Failed to save DataFrame to {path}") from e
    # Else success...
    else:
        print(" success!")