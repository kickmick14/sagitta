"""
" Dataframe manipulation functionality
"
" @author: Michael Kane
" @date:   08/09/2025
"""


def subDataframe(
        dataframe,
        features
        ):
    """
    Takes a larger dataframe, iterates through it and creates
    a new dataframe with the columns from the list

    Args:
        dataframe:
            A pandas dataframe.
        features:
            A list of column names within 'dataframe'
    Returns:
        subDataframe:
            A subset of 'dataframe' which has the columns as
            defined within 'features'
    """

    # Loop through parent dataframe and extract all columns in 'features'
    return dataframe[ [ feature for feature in features ] ]