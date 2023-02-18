

import numpy as np
import pandas as pd
from datetime import timedelta
from NILM_datasets.utilities import convert_object2timestamps


def __generate_activity_report(df, target_appliance, threshold_x, threshold_y):
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
            convert_object2timestamps(df)

        df_tmp = df[[target_appliance]].copy()
        mask = df[target_appliance] > threshold_y
        df_tmp['mask'] = (mask)
        df_tmp['cum_sum'] = (~mask).cumsum()
        df_tmp = df_tmp[df_tmp['mask'] == True]
        df_tmp = df_tmp.groupby(['cum_sum', str(df.index.name)]).first()

        for x in df_tmp.index.unique(level='cum_sum'):
            d = df_tmp.loc[(x)].reset_index()
            duration_start.append(d.iloc[0][str(df.index.name)])
            duration_end.append(d.iloc[-1][str(df.index.name)])
            duration_size.append(duration_end[-1] - duration_start[-1])
        durations = (pd.Series(duration_size)) / np.timedelta64(1, 's')
        activities =  pd.DataFrame({'start': duration_start, 'end': duration_end, 'duration_in_seconds': durations})

        prev_act_end = activities['end'][:-1]
        next_act_st = activities['start'][1:]
        prev_act_end = prev_act_end.reset_index()
        prev_act_end.drop(columns=['index'], inplace=True)
        next_act_st = next_act_st.reset_index()
        next_act_st.drop(columns=['index'], inplace=True)

        delta = next_act_st[next_act_st.columns[-1]] - prev_act_end[prev_act_end.columns[-1]]

        mask = delta < timedelta(minutes=30)
        mask = mask.shift(periods=1, fill_value=True)
        activities['low_delta'] = (mask)
        activities['cum_sum'] = (~mask).cumsum()

        start = activities.groupby(['cum_sum'])['start'].first()
        end = activities.groupby(['cum_sum'])['end'].last()
        duration_in_seconds = activities.groupby(['cum_sum'])['duration_in_seconds'].sum()
        duration_df = pd.DataFrame({'activity_start': start, 'activity_end': end, 'duration_in_seconds': duration_in_seconds})
        return duration_df[duration_df['duration_in_seconds']>30.0]
    
    except Exception as e:
        print("Exception raised in generate_activity_report() method = ", e)


        
def get_activities(data, target_appliance=None, threshold_x=None, threshold_y=None):
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

        if isinstance(data, dict):
            house_activities = {}
            for key, df in data.items():

                if target_appliance is None:
                    if len(df.columns)==2:
                        if df.columns[-1] != 'aggregate':
                            target_appliance = df.columns[-1]
                        else:
                            target_appliance = df.columns[0]
                    else:
                        raise Exception(f"Please specify target appliance {df.columns}")
                if threshold_y is None:
                    threshold_y = 0.02 * df[target_appliance].max()
                    print(f'Consumption Threshold is set to = {threshold_y}')
                if threshold_x is None:
                    if target_appliance == 'kettle':
                        threshold_x = 5.0
                    else:
                        threshold_x = 30.0
                    print(f'Time Delay Threshold is set to = {threshold_x} minutes')
                print(f"Estimating active durations of House {key}: {target_appliance}")
                house_activities.update({key: __generate_activity_report(df, target_appliance, threshold_x, threshold_y)})

            return house_activities
        
        elif isinstance(data, pd.core.frame.DataFrame):

            if target_appliance is None:
                if len(data.columns)==2:
                    if data.columns[-1] != 'aggregate':
                        target_appliance = data.columns[-1]
                    else:
                        target_appliance = data.columns[0]
                else:
                    raise Exception(f"Please specify target_appliance \n {data.columns}")
            if threshold_y is None:
                threshold_y = 0.02 * data[target_appliance].max()
                print(f'Consumption Threshold is set to = {threshold_y}')
            if threshold_x is None:
                if target_appliance == 'kettle':
                    threshold_x = 5.0
                else:
                    threshold_x = 30.0
                print(f'Time Delay Threshold is set to = {threshold_x} minutes')
            print(f"Estimating active durations of: {target_appliance}")
            return __generate_activity_report(data, target_appliance, threshold_x, threshold_y)

        else:
            print(f"Provided data should be of type <dict> or <pandas.core.frame.DataFrame> and not {type(data)}.")
    
    except Exception as e:
        print("Exception raised in get_activities() method = ", e)