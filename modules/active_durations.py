

import numpy as np
import pandas as pd
from refit_loader.utilities import convert_object2timestamps


def __generate_activity_report(df, target_appliance, threshold):
    """
    This method will return the durations or events when the appliance was active (on) 
    
    Parameters 
    ----------
    data : pandas.core.frame.DataFrame
            example dataframe is of the following format            
            {
                'time': pandas.core.indexes.datetimes.DatetimeIndex
                    timestamps as index identifying every data row
                'aggregate': numpy.int64
                    aggregated power consumption of all appliances in the specified house (watts)
                'kettle': numpy.int64
                    kettle power consumption in the specified house (watts)
            }
    target_appliance : string
            name of the target appliance (may be the name of the column targeted)
    threshold : float
            value of threshold for raw samples of power consumption, above this threshold appliance is consider active
        
    returns: pandas.core.frame.DataFrame
            dataframe is of the following format            
            {
                'Activity_Start': pandas._libs.tslibs.timestamps.Timestamp
                    start of the duration/event 
                'Activity_End': pandas._libs.tslibs.timestamps.Timestamp 
                    end of the duration/event
                'Duration': float
                    minutes of active appliance (using the method = convert_timestamps2minutes to convert timestamps to minutes)
            }
    """
    try: 
        duration_start = []
        duration_end = []
        duration_size = []

        if isinstance(df.index, object):
            df.index = convert_object2timestamps(df.index)

        df_tmp = df[[target_appliance]].copy()
        mask = df[target_appliance] > threshold
        df_tmp['mask'] = (mask)
        df_tmp['cum_sum'] = (~mask).cumsum()
        df_tmp = df_tmp[df_tmp['mask'] == True]
        df_tmp = df_tmp.groupby(['cum_sum', str(df.index.name)]).first()

        for x in df_tmp.index.unique(level='cum_sum'):
            d = df_tmp.loc[(x)].reset_index()
            duration_start.append(d.iloc[0][str(df.index.name)])
            duration_end.append(d.iloc[-1][str(df.index.name)])
            duration_size.append(duration_end[-1] - duration_start[-1])
        durations = (pd.Series(duration_size)) / np.timedelta64(1, 'm')
        return pd.DataFrame({'Activity_Start': duration_start, 'Activity_End': duration_end, 'Duration': durations})
    
    except Exception as e:
        print("Exception raised in generate_activity_report() method = ", e)


        
def get_activities(data, target_appliance=None, threshold=None):
    """
    This method will call the generate_activity_report for every dataframe to compute the durations of the active appliance and append to either a dictionary or returns back the single dataframe
    
    Parameters 
    ----------
    data : dict or pandas.core.frame.DataFrame
                 dictionary =
                         contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                         every dataframe includes the aggregate consumption and power consumption of a specific appliance in float values
                         with timestamps as index
                 pandas.core.frame.DataFrame  =
                         single dataframe of a single house that includes the aggregate consumption and power consumption of a specific appliance in float values
                         with timestamps as index
                         example dataframe is of the following format            
                            {
                                'time': pandas.core.indexes.datetimes.DatetimeIndex
                                    timestamps as index identifying every data row
                                'aggregate': numpy.int64
                                    aggregated power consumption of all appliances in the specified house (watts)
                                'kettle': numpy.int64
                                    kettle power consumption in the specified house (watts)
                            }
                     
    target_appliance : string
            name of the target appliance (may be the name of the column targeted)
    threshold : float
            value of threshold for raw samples of power consumption, above this threshold appliance is consider active 
        
    returns: dict or pandas.core.frame.DataFrame
                dictionary =
                        contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                pandas.core.frame.DataFrame  =
                        dataframe is of the following format            
                            {
                                'Activity_Start': pandas._libs.tslibs.timestamps.Timestamp
                                    start of the duration/event 
                                'Activity_End': pandas._libs.tslibs.timestamps.Timestamp 
                                    end of the duration/event
                                'Duration': float
                                    minutes of active appliance (using the method = convert_timestamps2minutes to convert timestamps to minutes)
                            }
    """
    try:
        if threshold is None:
                threshold = 0.0
        
        if isinstance(data, dict):
            house_activities = {}
            for key, df in data.items():
                
                if target_appliance is None:
                    if len(df.columns)==2:
                        target_appliance = df.columns[-1]
                    else:
                        raise Exception(f"Please specify target appliance {df.columns}")
                    
                print(f"Estimating active durations of House {key}: {target_appliance}")
                house_activities.update({key: __generate_activity_report(df, target_appliance, threshold)})

            return house_activities
        
        elif isinstance(data, pd.core.frame.DataFrame):
            
            if target_appliance is None:
                if len(data.columns)==2:
                    target_appliance = data.columns[-1]
                else:
                    raise Exception(f"Please specify target_appliance \n {data.columns}")
            print(f"Estimating active durations of: {target_appliance}")
            return __generate_activity_report(data, target_appliance, threshold)

        else:
            print(f"Provided data should be of type <dict> or <pandas.core.frame.DataFrame> and not {type(data)}.")
    
    except Exception as e:
        print("Exception raised in get_activities() method = ", e)