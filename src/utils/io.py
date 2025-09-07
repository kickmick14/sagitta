"""
" Functions to load files
"
" @author: Michael Kane
" @date:   07/09/2025
"""
import json, os


def loadJSON(
        path
        ):
    """
    Helper to load JSON file

    Args:
        path:
            File location
    Returns:
        file:
            Loaded file
    """

    # Check if location exists
    if os.path.exists( path ):
        # Open file
        f = open( path, "r" )
        # Load file
        file = json.load( f )
        # Close file
        f.close()
        
        return file
    else:
        return ImportError