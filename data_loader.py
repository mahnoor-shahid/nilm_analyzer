
import pandas as pd
import os, pickle
from sklearn.preprocessing import StandardScaler
import math
import dask.dataframe as dd
from refit_loader.utilities.configuration import get_config_from_json
from refit_loader.utilities.parser import refit_parser
from refit_loader.utilities.time_utils import convert_object2timestamps
from refit_loader.utilities.validations import check_house_availability, check_list_validations, check_correct_datatype
from refit_loader.utilities.active_durations import get_activities
    
class __Loader:
    """
    Interface that loads all the data into the memory
    """

    def __init__(self):
        try:
            pass
            
        except Exception as e:
            print("Error occured in initialization of _Loader interface due to ", e)
                
        finally:
            pass
        
    @staticmethod
    def _load_file():
        raise NotImplementedError    


        
class CSV_Loader(__Loader):
    """
    Class that loads all the data into the memory using different methods
    """
    def __init__(self):
        try:
            super().__init__()
        
        except Exception as e:
            print("Error occured in initialization of CSV_Loader class due to ", e)
                
        finally:
            pass
        
    @staticmethod
    def _load_file(csv_file_path,
                   index_column_name=None,
                   _nrows=None,
                   _iterator=True,
                   _chunksize=100000):
        try:
            tp = pd.read_csv(csv_file_path, nrows=_nrows, index_col=index_column_name, iterator=_iterator, chunksize=_chunksize) ## loading data in chunks reduces 90 percent execution time 
            df = pd.concat(tp, ignore_index=False)
            df.info(verbose=False, memory_usage="deep")
            return df  
        
        except Exception as e:
            print("Error occured in _load_file method of CSV_Loader class due to ", e)
    
    @staticmethod
    def _load_files_via_dask(_data_folder,
                             _files_format,
                             _buildings):
        try:
            ls = {}
            print(f"\nLoading specified buildings: {_buildings}")
            for i in _buildings:
                ls.update({i: dd.read_csv(f"{_data_folder}{i}{_files_format}")})
            return ls
        
        except Exception as e:
            print("Error occured in _load_file_via_dask method of CSV_Loader class due to ", e)
    

    
class REFIT_Loader(CSV_Loader):
    """
    Class that loads all the data into the memory using DASK
    """
    def __init__(self):
        try:
            super().__init__()
        
        except Exception as e:
            print("Error occured in initialization of REFIT_Loader class due to ", e)
                
        finally:
            self.__config = get_config_from_json(description="refit_loader configuration", config_file="refit_loader/config.json")
            self.__collective_dataset = CSV_Loader._load_files_via_dask(_data_folder=self.__config['DATA_FOLDER']+'House_',
                                                                _files_format=self.__config['DATA_TYPE'],
                                                                _buildings=self.__config['REFIT_HOUSES'])
            self.__keys_of_appliances = refit_parser(self.__config['README_FILE'])
            for house_number in self.__collective_dataset:
                cols = [header.lower() for header in self.__keys_of_appliances[str(house_number)]]
                self.__collective_dataset[house_number] = self.__collective_dataset[house_number].rename(columns={"Time": "time", "Unix": "unix", "Aggregate": cols[0], "Appliance1":cols[1], "Appliance2":cols[2],
                                                                                                      "Appliance3":cols[3], "Appliance4":cols[4], "Appliance5":cols[5],"Appliance6":cols[6], "Appliance7":cols[7],
                                                                                                      "Appliance8":cols[8], "Appliance9":cols[9]})
                self.__collective_dataset[house_number].index = self.__collective_dataset[house_number]['time']
                self.__collective_dataset[house_number] = self.__collective_dataset[house_number].drop('time', axis=1)
                
    def get_appliance_names(self, house: int):
        """
        This method will return the names of the available appliances in the specified house number

        Parameters 
        ----------
        house : int 
                number of a specific house e.g, house=2 for 'house_2.csv' 

        returns: list
                 contains names of the available appliances in the specified house
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__collective_dataset.keys()):
                    print(f"Fetching appliances for house = {house}")
                    return [name for name in self.__collective_dataset[house].columns]
        except Exception as e:
            print("Error occured in get_appliance_names method of REFIT_Loader due to ", e)
                
    def get_house_data(self, house: int):
        """
        This method will return the dataframe of the specified house number from refit dataset

        Parameters 
        ----------
        house : int 
                number of a specific house e.g, house=2 for 'house_2.csv' 

        returns: pandas.core.frame.DataFrame
                dataframe is of the following format            
                {
                    'time': pandas.core.indexes.datetimes.DatetimeIndex
                        timestamps as index identifying every data row
                    'unix': numpy.int64
                        timestamps in unix
                    'aggregate': numpy.int64
                        aggregated power consumption of all appliances in the sepcified house
                    
                    *** appliances and their consumption values in numpy.int64 varies house wise ***
                }
        """
        try:                
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__collective_dataset.keys()):
                    print(f"Loading data for house = {house}")
                    data = self.__collective_dataset[house].compute() 
                    data.index = convert_object2timestamps(data.index)
                    data = data.loc[:, data.columns != "unix"].astype(float)
                    return data
        
        except Exception as e:
            print("Error occured in get_house_data method of REFIT_Loader due to ", e)
    
    def get_appliance_data(self, appliance: str, houses=None):
        """
        This method will return RefitData object that can let user access data in dictionary format as well can access some transformation methods

        Parameters 
        ----------
        appliance : string
                name of the target appliance (name of the column targeted in the specified house/s)
        house : list
                contains numbers of a specific houses e.g, house=2 for 'house_2.csv' 

        returns: RefitData object
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format            
                        {
                            'time': pandas.core.indexes.datetimes.DatetimeIndex
                                timestamps as index identifying every data row
                            'unix': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the sepcified house

                            *** appliances and their consumption values in numpy.int64 varies house wise ***
                        }
        """
        try:
            self.__data = {}
            if check_correct_datatype(arg_name='appliance', arg_value=appliance, target_datatype=str):
                target_appliance = appliance.lower()
            if houses == None:
                houses=list(self.__collective_dataset.keys())
                
            print(f"Loading data for appliance {target_appliance.upper()} ...")
            if check_list_validations(arg_name='houses', arg_value=houses, member_datatype='int'):
                for house_number in houses:
                    if check_house_availability(arg_name='House Number', arg_value=house_number, collection=self.__collective_dataset.keys()):
                        if target_appliance in self.__collective_dataset[house_number].columns:
                            if house_number not in self.__data.keys():
                                print(f"Fetching {target_appliance.upper()} data for House {house_number}")
                                data = self.__collective_dataset[house_number][['aggregate', target_appliance]].compute()
                                data.index = convert_object2timestamps(data.index)
                                data = data.astype(float)
                                self.__data.update({house_number: data})
                        else:
                            print(f"Appliance '{target_appliance.upper()}' does not exist in house {house_number}.")

            return RefitData(self.__data)
                
        except Exception as e:
            print("Error occured in get_appliance_data method of REFIT_Loader due to ", e)


class RefitData():
    """
    Class that loads the provided data after computing DASK dataframes and provides different methods for transformations
    """
    def __init__(self, data):
        try:
            self.data = data
        
        except Exception as e:
            print("Error occured in initialization of RefitData class due to ", e)
                
        finally:
            pass
    
    def resample(self, sampling_period='8s', window_limit=3.0, fill_value=0.0):
        """
        This method will return RefitData object that can let user access data in dictionary format as well can access some transformation methods

        Parameters 
        ----------
        house : int 
                number of a specific house e.g, house=2 for 'house_2.csv' 
        sampling_period: string
                         set the sampling rate in a string format e.g, '8s' means 8 seconds
        window_limit : float
                        set the window size in minutes to forward fill last value
        fill_value: float
                    set the value with which remaining np.nans are filled 

        returns: RefitData object (updated)
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format            
                        {
                            'time': pandas.core.indexes.datetimes.DatetimeIndex
                                timestamps as index identifying every data row
                            'unix': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the sepcified house

                            *** appliances and their consumption values in numpy.int64 varies house wise ***
                        }
        """
        try:
            self.__sampling_period = sampling_period
            self.__fill_value = fill_value
            self.__window_limit= int(window_limit*60)

            for house_number in self.data.keys():
                print(f"Resampling for house number: ", house_number)
#                     target_appliance = self.data[house_number].columns[-1]
                appliance_data = self.data[house_number]
#                     appliance_data = appliance_data.resample('1s').mean().dropna()
                appliance_data = appliance_data.resample('1s').asfreq()
                appliance_data.fillna(method='ffill', axis=0, inplace=True, limit=self.__window_limit)
                appliance_data.fillna(axis=0, inplace=True, value=self.__fill_value)
                appliance_data = appliance_data.resample(self.__sampling_period).median()
                appliance_data.dropna(inplace = True)
                self.data.update({house_number: appliance_data})
            print("Updating data with resampled dataset...")

        except Exception as e:
            print("Error occured in resample method of REFIT_Data due to ", e)
            
            
    def subset_data(self, no_of_days=5 ):

            """
            This method will create different and smaller versions of the training, validation and testing subsets from the collective_data

            Parameters 
            ----------
            no_of_days: int
                            number of days with active appliance time/durations

            returns: RefitData object (updated)
                .active_data = to access subset_data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format            
                        {
                            'time': pandas.core.indexes.datetimes.DatetimeIndex
                                timestamps as index identifying every data row
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the sepcified house
                            *** target_appliance : numpy.int64
                                power consumption of target appliances in the sepcified house
                        }
            """
            self.__no_of_days = no_of_days
            for house_number, value in self.data.items():
                print(f"Creating {self.__no_of_days} smaller subset/s from complete dataset of House {house_number}")
                activities = get_activities(value)
                date_wise_activities = activities.groupby([activities['Activity_Start'].dt.date]).mean()
                time_indices = date_wise_activities.sort_values('Duration').tail(self.__no_of_days).index
                df_outer = pd.DataFrame()
                for version, time_indx in enumerate(time_indices):
                    df_outer = pd.concat([df_outer, value.loc[str(time_indx)]])
                self.data.update({house_number: df_outer})
            print("Updating data with selected active appliance activities...")
            
            
    def get_proportioned_data(self, splits_proportion):
        """
        """
        try:
            self.__proportion = splits_proportion
            
            __all_data = pd.DataFrame()
            for house_number in self.data.keys():
                __all_data = pd.concat([__all_data, self.data[house_number]])
            __train_end = __all_data.index[math.floor(self.__proportion['TRAIN_PERCENT'] * len(__all_data))]
            __val_end = __all_data.index[math.floor((self.__proportion['TRAIN_PERCENT'] + self.__proportion['VALIDATE_PERCENT']) * len(__all_data))]
            print("Updating data with proportioned splits...")
            self.data = {'TRAIN_SPLIT': __all_data[:__train_end] , 'VALIDATE_SPLIT': __all_data[__train_end:__val_end], 'TEST_SPLIT': __all_data[__train_end:__val_end]}

        except Exception as e:
            print("Error occured in get_proportioned_data method due to ", e)

    def normalize(self, target_houses, scaler, scalars_directory, training=False ):
        """
        This method will standardize the values of the provided dataset. It will compute the scalars on trainset and save them to provided path which can later be used to standarize validation set

        Parameters
        ----------
        train_flag : bool
                to indicate the provided dataframe is of training dataset
        df: pandas.core.frame.DataFrame
                dataframe is of the following format
                {
                    'time': pandas.core.indexes.datetimes.DatetimeIndex
                        timestamps as index identifying every data row
                    'aggregate': numpy.int64
                        aggregated power consumption of all appliances in the sepcified house
                    * any target appliance (vary) *: numpy.int64
                        aggregated power consumption of all appliances in the sepcified house
                }

        returns: scaled_df: pandas.core.frame.DataFrame

        """
        try:
            

            if scaler == 'Standard':
                X_scaler, y_scaler = StandardScaler(), StandardScaler()

            if not os.path.exists(scalars_directory):
                os.makedirs(f'{scalars_directory}/', )  # if not then create folder

            if training:
                self.__target_houses = target_houses
                if not ((self.__target_houses['TRAIN']== self.__target_houses['VALIDATE']) and (self.__target_houses['TRAIN'] == self.__target_houses['TEST'])):

                    __train_split = pd.DataFrame()
                    for house_number in self.__target_houses['TRAIN']:
                        __train_split = pd.concat([__train_split, self.data[house_number]])

                    __val_split = pd.DataFrame()
                    for house_number in self.__target_houses['VALIDATE']:
                        __val_split = pd.concat([__val_split, self.data[house_number]])

                    __test_split = pd.DataFrame()
                    for house_number in self.__target_houses['TEST']:
                        __test_split = pd.concat([__test_split, self.data[house_number]])  
                else:
                    __train_split = self.data['TRAIN_SPLIT'] 
                    __val_split = self.data['VALIDATE_SPLIT']
                    __test_split = self.data['TEST_SPLIT']

                X_array = __train_split['aggregate'].values
                y_array = __train_split[__train_split.columns[-1]].values

                X_scaler.fit(X_array.reshape(-1, 1))
                y_scaler.fit(y_array.reshape(-1, 1))

                pickle.dump(X_scaler, open(os.path.join(scalars_directory, f'X_scaler.sav'), 'wb'))
                pickle.dump(y_scaler, open(os.path.join(scalars_directory, f'y_scaler.sav'), 'wb'))

                X_train = X_scaler.transform(X_array.reshape(-1, 1)).flatten()
                y_train = y_scaler.transform(y_array.reshape(-1, 1)).flatten()

                __train_df = pd.DataFrame({'time': __train_split.index, 'aggregate': X_train, f"{__train_split.columns[-1]}":y_train}).set_index('time')

                X_array = __val_split['aggregate'].values
                y_array = __val_split[__val_split.columns[-1]].values

                X_val = X_scaler.transform(X_array.reshape(-1, 1)).flatten()
                y_val = y_scaler.transform(y_array.reshape(-1, 1)).flatten()

                X_array = __test_split['aggregate'].values
                y_array = __test_split[__test_split.columns[-1]].values

                X_test = X_scaler.transform(X_array.reshape(-1, 1)).flatten()
                y_test = y_scaler.transform(y_array.reshape(-1, 1)).flatten()

                __val_df = pd.DataFrame({'time': __val_split.index, 'aggregate': X_val, f"{__val_split.columns[-1]}":y_val}).set_index('time')
                __test_df = pd.DataFrame({'time': __test_split.index, 'aggregate': X_test, f"{__test_split.columns[-1]}":y_test}).set_index('time')

                self.data = {'TRAIN_SPLIT': __train_df, 'VALIDATE_SPLIT': __val_df, 'TEST_SPLIT': __test_df}
            
            else:
                for house_number in self.data.keys():
                    print(f"Normalizing for house number: ", house_number)
                    __appliance_data = self.data[house_number]
                    X_array = __appliance_data['aggregate'].values
                    y_array = __appliance_data[__appliance_data.columns[-1]].values

                    X_scaler.fit(X_array.reshape(-1, 1))
                    y_scaler.fit(y_array.reshape(-1, 1))

                    pickle.dump(X_scaler, open(os.path.join(scalars_directory, f'X_scaler.sav'), 'wb'))
                    pickle.dump(y_scaler, open(os.path.join(scalars_directory, f'y_scaler.sav'), 'wb'))

                    X_train = X_scaler.transform(X_array.reshape(-1, 1)).flatten()
                    y_train = y_scaler.transform(y_array.reshape(-1, 1)).flatten()
                    __df = pd.DataFrame({'time': __train_split.index, 'aggregate': X_train, f"{__train_split.columns[-1]}":y_train}).set_index('time')
                    self.data.update({house_number: __normalized_df})

        except Exception as e:
            print("Error occured in normalize method of REFIT_Data due to ", e)