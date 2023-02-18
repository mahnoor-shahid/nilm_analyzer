from os.path import isdir, dirname, abspath
from inspect import currentframe, getfile, getsourcefile
from os import getcwd
from sys import getfilesystemencoding
import pandas as pd
import numpy as np


def get_module_directory():
    # Taken from http://stackoverflow.com/a/6098238/732596
    path_to_this_file = dirname(getfile(currentframe()))
    if not isdir(path_to_this_file):
        encoding = getfilesystemencoding()
        path_to_this_file = dirname(unicode(__file__, encoding))
    if not isdir(path_to_this_file):
        abspath(getsourcefile(lambda _: None))
    if not isdir(path_to_this_file):
        path_to_this_file = getcwd()
    assert isdir(path_to_this_file), path_to_this_file + ' is not a directory'
    return path_to_this_file


def convert_timestamps2minutes(tstamps: pd.Series):
    try:
        return tstamps / np.timedelta64(1, 'm')

    except Exception as e:
        print("Exception raised in generating convert_timestamps2minutes() method = ", e)


def convert_object2timestamps(df, unit_value='s'):
    try:
        df.index = pd.to_datetime(df.index, unit=unit_value)

    except Exception as e:
        print("Exception raised in generating convert_objects2timestamps() method = ", e)