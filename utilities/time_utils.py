
import pandas as pd
import numpy as np


def convert_timestamps2minutes(tstamps: pd.Series):
    try:
        return tstamps / np.timedelta64(1, 'm')

    except Exception as e:
        print("Exception raised in generating convert_timestamps2minutes() method = ", e)
        
        
def convert_object2timestamps(objects: pd.Series):
    try:
        return pd.to_datetime(objects)

    except Exception as e:
        print("Exception raised in generating convert_objects2timestamps() method = ", e)