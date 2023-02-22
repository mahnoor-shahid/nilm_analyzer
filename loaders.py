import numpy as np
import pandas as pd
import os, pickle
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
import math
import glob
import dask.dataframe as dd
import warnings
warnings.filterwarnings('ignore')
from nilm_analyzer.modules.parser import refit_parser, ukdale_parser, ampds_parser, iawe_parser, deddiag_parser, gelap_parser
from nilm_analyzer.modules.validations import check_house_availability, check_list_validations, check_correct_datatype
from nilm_analyzer.modules.active_durations import get_activities
from nilm_analyzer.utilities import convert_object2timestamps, get_module_directory
    
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
                ls.update({i: dd.read_csv(os.path.join(_data_folder, f"House_{i}{_files_format}"))})
            return ls
        
        except Exception as e:
            print("Error occured in _load_file_via_dask method of CSV_Loader class due to ", e)

    @staticmethod
    def _load_files_via_dask_channel_wise(_data_folder, _metadata,
                                    _files_format,
                                    _buildings,
                                          _dataset):
        try:
            all_data = {}
            print(f"\nLoading specified buildings: {_buildings}")

            for building in _buildings:
                building_lst = []
                metadata = _metadata[int(building)]

                if _dataset == 'ukdale':
                    for row in range(0, len(metadata)):
                        meter_number = metadata.iloc[row]['meter']
                        appliance = metadata.iloc[row]['appliance']
                        ddf = dd.read_csv(
                            os.path.join(_data_folder, f"house_{building}", f"channel_{meter_number}{_files_format}"),
                            delim_whitespace=True, names=['time', appliance])
                        building_lst.append(ddf)
                    all_data.update({building: building_lst})

                elif _dataset == 'iawe':
                    for row in range(0, len(metadata)):
                        meter_number = metadata.iloc[row]['meter']
                        appliance = metadata.iloc[row]['appliance']
                        ddf = dd.read_csv(
                                os.path.join('D:/data/external/iawe', f"{meter_number}{_files_format}"),
                                dtype={'A': 'object',
                                       'PF': 'object',
                                       'VA': 'object',
                                       'VAR': 'object',
                                       'VLN': 'object',
                                       'W': 'object',
                                       'f': 'object'}
                            )

                        ddf = ddf.rename(columns={'timestamp': 'time', 'W': appliance})
                        building_lst.append(ddf[['time', appliance]])
                    all_data.update({building: building_lst})

                elif _dataset == 'deddiag':
                    for file in glob.glob(os.path.join(_data_folder, f"house_{building}", f"item_*_data{_files_format}")):
                        item = file.split('item_00')[-1].split('_data.tsv')[0]
                        appliance = metadata['appliance'][metadata['meter'] == int(item)].item()
                        df = dd.read_csv(file, sep='\t')
                        df = df.rename(columns={ 'value':appliance})
                        df = df.drop(columns=['item_id'])
                        building_lst.append(df)
                    all_data.update({int(building): building_lst})

                elif _dataset == 'gelap' :
                    for sm_file in glob.glob(
                            os.path.join(_data_folder, f"hh-{building}", f"smartmeter{_files_format}")):
                        sm = dd.read_csv(sm_file)
                        sm["aggregate"] = sm['power1'] + sm['power2'] + sm['power3']
                        sm = sm[['time', 'aggregate']]
                        building_lst.append(sm)

                        for file in glob.glob(
                                os.path.join(_data_folder, f"hh-{building}", f"label_*{_files_format}")):
                            item = file.split(f"label_")[-1].split('.csv')[0]
                            appliance = metadata[metadata['labels'] == item]['appliance'].item()
                            df = dd.read_csv(file)
                            df = df.rename(columns={'power': appliance, 'time_reply': 'time'})
                            df = df[['time', appliance]]
                            building_lst.append(df)
                    all_data.update({int(building): building_lst})

                else:
                    pass

            return all_data
        except Exception as e:
            print("Error occured in _load_files_via_dask_channel_wise method of CSV_Loader class due to ", e)

    
class REFIT_Loader(CSV_Loader):
    """
    Class that loads all the REFIT data into the memory using DASK and provides different methods to fetch subset of dataset from complete origial.
    """
    def __init__(self, data_path):
        try:
            super().__init__()
        
        except Exception as e:
            print("Error occured in initialization of REFIT_Loader class due to ", e)
                
        finally:
            # self.__config = get_config_from_json(description="refit_loader configuration", config_file='../refit_loader/config.json')
            self.__config = {'DATA_FOLDER': os.path.join(os.getcwd(), data_path), 'DATA_TYPE':'.csv', 'METADATA': os.path.join(get_module_directory(), 'metadata', 'refit'),
                             'REFIT_HOUSES': [1,2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,20,21]}
            self.__keys_of_appliances = refit_parser(_buildings=self.__config['REFIT_HOUSES'], _metadata=self.__config['METADATA'])
            self.__collective_dataset = CSV_Loader._load_files_via_dask(_data_folder=self.__config['DATA_FOLDER'],
                                                                _files_format=self.__config['DATA_TYPE'],
                                                                _buildings=self.__config['REFIT_HOUSES'])

            if self.__collective_dataset:
                print('Dataset successfully loaded!')
            else:
                print('Dataset was not loaded successfully!')

            for house_number in self.__config['REFIT_HOUSES']:
                cols = [header.lower() for header in self.__keys_of_appliances[house_number]['appliance']]
                self.__collective_dataset[house_number] = self.__collective_dataset[house_number].rename(columns={"Time": "timestamps", "Unix": "time", "Aggregate": 'aggregate', "Appliance1":cols[0], "Appliance2":cols[1],
                                                                                                          "Appliance3":cols[2], "Appliance4":cols[3], "Appliance5":cols[4],"Appliance6":cols[5], "Appliance7":cols[6],
                                                                                                          "Appliance8":cols[7], "Appliance9":cols[8]})
                # self.__collective_dataset[house_number].index = self.__collective_dataset[house_number]['time']
                # self.__collective_dataset[house_number] = self.__collective_dataset[house_number].drop('time', axis=1)
                
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
                    return [header.lower() for header in self.__keys_of_appliances[house]['appliance']]
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
                    data.set_index('time', inplace=True)
                    convert_object2timestamps(data)
                    data = data.loc[:, data.columns != "timestamps"].astype(float)
                    return data
        
        except Exception as e:
            print("Error occured in get_house_data method of REFIT_Loader due to ", e)
    
    def get_appliance_data(self, appliance: str, houses=None):
        """
        This method will return EnergyData object that can let user access data in dictionary format as well can access some transformation methods

        Parameters 
        ----------
        appliance : string
                name of the target appliance (name of the column targeted in the specified house/s)
        house : list
                contains numbers of a specific houses e.g, house=2 for 'house_2.csv' 

        returns: _EnergyDataset object
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format            
                        {
                            'time': pandas.core.indexes.datetimes.DatetimeIndex
                                timestamps as index identifying every data row
                            'unix': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the specified house

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
                                data = self.__collective_dataset[house_number][['aggregate', 'time', target_appliance]].compute()
                                data.set_index('time', inplace=True)
                                convert_object2timestamps(data)
                                data = data.astype(float)
                                self.__data.update({house_number: data})
                        else:
                            print(f"Appliance '{target_appliance.upper()}' does not exist in house {house_number}.")

            return _EnergyDataset(self.__data)
                
        except Exception as e:
            print("Error occured in get_appliance_data method of REFIT_Loader due to ", e)

class UKDALE_Loader(CSV_Loader):
    """
    Class that loads all the UKDALE data into the memory using DASK and provides different methods to fetch subset of dataset from complete origial.
    """
    def __init__(self, data_path):
        try:
            super().__init__()

        except Exception as e:
            print("Error occured in initialization of UKDALE_Loader class due to ", e)

        finally:
            self.__config = {'DATA_FOLDER': os.path.join(os.getcwd(), data_path), 'DATA_TYPE':'.dat', 'METADATA': os.path.join(get_module_directory(), 'metadata', 'ukdale'),
                             'UKDALE_HOUSES': [1,2,3,4,5]}
            self.__keys_of_appliances = ukdale_parser(_buildings=self.__config['UKDALE_HOUSES'], _metadata=self.__config['METADATA'])
            self.__collective_dataset = CSV_Loader._load_files_via_dask_channel_wise(_data_folder=self.__config['DATA_FOLDER'],
                                                                        _metadata=self.__keys_of_appliances,
                                                                _files_format=self.__config['DATA_TYPE'],
                                                                _buildings=self.__config['UKDALE_HOUSES'],
                                                                                     _dataset='ukdale')

            if self.__collective_dataset:
                print('Dataset successfully loaded!')
            else:
                print('Dataset was not loaded successfully!')


    def get_appliance_names(self, house: int):
        """
        This method will return the names of the available appliances in the specified house number

        Parameters
        ----------
        house : int
                number of a specific house

        returns: list
                 contains names of the available appliances in the specified house
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Fetching appliances for house = {house}")
                    return [name.lower() for name in self.__keys_of_appliances[house]['appliance'][1:]]
        except Exception as e:
            print("Error occured in get_appliance_names method of UKDALE_Loader due to ", e)

    def get_house_data(self, house: int):
        """
        This method will return the dataframe of the specified house number from ukdale dataset

        Parameters
        ----------
        house : int
                number of a specific house/building

        returns: pandas.core.frame.DataFrame
                dataframe is of the following format
                {
                    'time': pandas.core.indexes.datetimes.DatetimeIndex
                        timestamps as index identifying every data row
                    'aggregate': numpy.int64
                        aggregated power consumption of all appliances in the specified house

                    *** appliances and their consumption values in numpy.int64 varies house wise ***
                }
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Loading data for house = {house}")
                    base = self.__collective_dataset[house][0]
                    for i in range(1, len(self.__collective_dataset[house])):
                        base = base.merge(self.__collective_dataset[house][i], how="left", on=["time"])
                    base = base.compute()
                    base.set_index('time', inplace=True)
                    convert_object2timestamps(base)
                    return base

        except Exception as e:
            print("Error occured in get_house_data method of UKDALE_Loader due to ", e)

    def get_appliance_data(self, appliance: str, houses=None):
        """
        This method will return _EnergyDataset object that can let user access data in dictionary format as well can access some transformation methods

        Parameters
        ----------
        appliance : string
                name of the target appliance (name of the column targeted in the specified house/s)
        house : list
                contains numbers of a specific houses e.g, house=2 for 'house_2.csv'

        returns: _EnergyDataset object
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format
                        {
                            'time': pandas.core.indexes.datetimes.DatetimeIndex
                                timestamps as index identifying every data row
                            'unix': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the specified house

                            *** appliances and their consumption values in numpy.int64 varies house wise ***
                        }
        """
        try:
            self.__data = {}
            if check_correct_datatype(arg_name='appliance', arg_value=appliance, target_datatype=str):
                target_appliance = appliance.lower()
            if houses == None:
                houses = list(self.__keys_of_appliances.keys())

            print(f"Loading data for appliance {target_appliance.upper()} ...")
            if check_list_validations(arg_name='houses', arg_value=houses, member_datatype='int'):
                for house_number in houses:
                    if check_house_availability(arg_name='House Number', arg_value=house_number,
                                                collection=self.__keys_of_appliances.keys()):
                        if target_appliance in [self.__collective_dataset[house_number][i].columns[-1] for i in range(0, len(self.__collective_dataset[house_number]))]:
                            if house_number not in self.__data.keys():
                                print(f"Fetching {target_appliance.upper()} data for House {house_number}")
                                base = self.__collective_dataset[house_number][0]
                                _query = 'appliance=="{}"'.format(target_appliance)
                                base = base.merge(self.__collective_dataset[house_number]
                                                  [self.__keys_of_appliances[house_number].query(_query)['meter'].item() - 1], how="left", on=["time"])
                                base = base.compute()
                                base.set_index('time', inplace=True)
                                convert_object2timestamps(base)
                                self.__data.update({house_number: base})
                        else:
                            print(f"Appliance '{target_appliance.upper()}' does not exist in house {house_number}.")

            return _EnergyDataset(self.__data)

        except Exception as e:
            print("Error occured in get_appliance_data method of UKDALE_Loader due to ", e)

class AMPDS_Loader(CSV_Loader):
    """
    Class that loads all the AMPDS data into the memory using DASK and provides different methods to fetch subset of dataset from complete origial.
    """

    def __init__(self, data_path):
        try:
            super().__init__()

        except Exception as e:
            print("Error occured in initialization of AMPDS_Loader class due to ", e)

        finally:
            self.__config = {'DATA_FOLDER': os.path.join(os.getcwd(), data_path), 'DATA_TYPE': '.csv',
                             'METADATA': os.path.join(get_module_directory(), 'metadata', 'ampds'),
                             'AMPDS_HOUSES': [1]}
            self.__keys_of_appliances = ampds_parser(_buildings=self.__config['AMPDS_HOUSES'],
                                                     _metadata=self.__config['METADATA'])
            for py in glob.glob(os.path.join(f"{self.__config['DATA_FOLDER']}",f"*{self.__config['DATA_TYPE']}")):
                print("\nLoading specified buildings: [1]")
                self.__collective_dataset = {self.__config['AMPDS_HOUSES'][0]: pd.read_csv(py)}


            if self.__collective_dataset:
                print('Dataset successfully loaded!')
            else:
                print('Dataset was not loaded successfully!')

            for house_number in self.__config['AMPDS_HOUSES']:
                prev_col = [header for header in self.__keys_of_appliances[house_number]['meter']]
                cols = [header for header in self.__keys_of_appliances[house_number]['appliance']]
                self.__collective_dataset[house_number] = self.__collective_dataset[house_number].rename(
                    columns={"UNIX_TS": "time", 'WHE':'aggregate',prev_col[0]: cols[0], prev_col[1]: cols[1], prev_col[2]: cols[2], prev_col[3]: cols[3], prev_col[4]: cols[4], prev_col[5]: cols[5],
                             prev_col[6]: cols[6], prev_col[7]: cols[7], prev_col[8]: cols[8], prev_col[9]: cols[9], prev_col[10]: cols[10], prev_col[11]: cols[11],
                             prev_col[12]: cols[12], prev_col[13]: cols[13], prev_col[14]: cols[14], prev_col[15]: cols[15], prev_col[16]: cols[16], prev_col[17]: cols[17],
                             prev_col[18]: cols[18],prev_col[19]: cols[19]})

    def get_appliance_names(self, house: int):
        """
        This method will return the names of the available appliances in the specified house number

        Parameters
        ----------
        house : int
                number of a specific house

        returns: list
                 contains names of the available appliances in the specified house
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house,
                                            collection=self.__collective_dataset.keys()):
                    print(f"Fetching appliances for house = {house}")
                    return [header.lower() for header in self.__collective_dataset[house].columns[1:]]
        except Exception as e:
            print("Error occured in get_appliance_names method of AMPDS_Loader due to ", e)

    def get_house_data(self, house: int):
        """
        This method will return the dataframe of the specified house number from ampds dataset

        Parameters
        ----------
        house : int
                number of a specific house e.g, house=2 for 'house_2.csv'

        returns: pandas.core.frame.DataFrame
                dataframe is of the following format
                {
                    'UNIX_TS': numpy.int64
                        timestamps in unix
                    'aggregate': numpy.int64
                        aggregated power consumption of all appliances in the specified house

                    *** appliances and their consumption values in numpy.int64 varies house wise ***
                }
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house,
                                            collection=self.__collective_dataset.keys()):
                    print(f"Loading data for house = {house}")
                    data = self.__collective_dataset[house].copy(deep=True)
                    data.set_index('time', inplace=True)
                    convert_object2timestamps(data)
                    data = data.loc[:, data.columns != "timestamps"].astype(float)
                    return data

        except Exception as e:
            print("Error occured in get_house_data method of AMPDS_Loader due to ", e)

    def get_appliance_data(self, appliance: str, houses=None):
        """
        This method will return EnergyData object that can let user access data in dictionary format as well can access some transformation methods

        Parameters
        ----------
        appliance : string
                name of the target appliance (name of the column targeted in the specified house/s)
        house : list
                contains numbers of a specific houses e.g, house=2 for 'house_2.csv'

        returns: _EnergyDataset object
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format
                        {
                            'UNIX_TS': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the specified house

                            *** appliances and their consumption values in numpy.int64 varies house wise ***
                        }
        """
        try:
            self.__data = {}
            if check_correct_datatype(arg_name='appliance', arg_value=appliance, target_datatype=str):
                target_appliance = appliance
            if houses == None:
                houses = list(self.__collective_dataset.keys())

            print(f"Loading data for appliance {target_appliance.upper()} ...")
            if check_list_validations(arg_name='houses', arg_value=houses, member_datatype='int'):
                for house_number in houses:
                    if check_house_availability(arg_name='House Number', arg_value=house_number,
                                                collection=self.__collective_dataset.keys()):
                        if target_appliance in self.__collective_dataset[house_number].columns:
                            if house_number not in self.__data.keys():
                                print(f"Fetching {target_appliance.upper()} data for House {house_number}")
                                data = self.__collective_dataset[house_number]
                                data.set_index('time', inplace=True)
                                convert_object2timestamps(data)
                                data = data[['aggregate', target_appliance]]
                                data = data.astype(float)
                                self.__data.update({house_number: data})
                        else:
                            print(f"Appliance '{target_appliance.upper()}' does not exist in house {house_number}.")

            return _EnergyDataset(self.__data)

        except Exception as e:
            print("Error occured in get_appliance_data method of AMPDS_Loader due to ", e)

class IAWE_Loader(CSV_Loader):
    """
    Class that loads all the IAWE data into the memory using DASK and provides different methods to fetch subset of dataset from complete origial.
    """
    def __init__(self, data_path):
        try:
            super().__init__()

        except Exception as e:
            print("Error occured in initialization of IAWE_Loader class due to ", e)

        finally:
            self.__config = {'DATA_FOLDER': os.path.join(os.getcwd(), data_path), 'DATA_TYPE':'.csv', 'METADATA': os.path.join(get_module_directory(), 'metadata', 'iawe'),
                             'IAWE_HOUSES': [1]}
            self.__keys_of_appliances = iawe_parser(_buildings=self.__config['IAWE_HOUSES'], _metadata=self.__config['METADATA'])
            self.__collective_dataset = CSV_Loader._load_files_via_dask_channel_wise(_data_folder=self.__config['DATA_FOLDER'],
                                                                        _metadata=self.__keys_of_appliances,
                                                                _files_format=self.__config['DATA_TYPE'],
                                                                _buildings=self.__config['IAWE_HOUSES'], _dataset='iawe')

            if self.__collective_dataset:
                print('Dataset successfully loaded!')
            else:
                print('Dataset was not loaded successfully!')


    def get_appliance_names(self, house: int):
        """
        This method will return the names of the available appliances in the specified house number

        Parameters
        ----------
        house : int
                number of a specific house

        returns: list
                 contains names of the available appliances in the specified house
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Fetching appliances for house = {house}")
                    return [name.lower() for name in self.__keys_of_appliances[house]['appliance'][2:]]
        except Exception as e:
            print("Error occured in get_appliance_names method of IAWE_Loader due to ", e)

    def get_house_data(self, house: int):
        """
        This method will return the dataframe of the specified house number from IAWE dataset

        Parameters
        ----------
        house : int
                number of a specific house/building

        returns: pandas.core.frame.DataFrame
                dataframe is of the following format
                {
                    'time': pandas.core.indexes.datetimes.DatetimeIndex
                        timestamps as index identifying every data row
                    'aggregate': numpy.int64
                        aggregated power consumption of all appliances in the specified house

                    *** appliances and their consumption values in numpy.int64 varies house wise ***
                }
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Loading data for house = {house}")
                    base = self.__collective_dataset[house][0]
                    for i in range(1, len(self.__collective_dataset[house])):
                        base = base.merge(self.__collective_dataset[house][i], how="left", on=["time"])
                    base = base.compute()
                    base['aggregate_x'][base['aggregate_x'] == "\\N"] = 0.0
                    base['aggregate_y'][base['aggregate_y'] == "\\N"] = 0.0
                    base['aggregate_x'] = base['aggregate_x'].astype('float')
                    base['aggregate_y'] = base['aggregate_y'].astype('float')
                    base['aggregate'] = base['aggregate_x'] + base['aggregate_y']
                    base['aggregate'] = base['aggregate'].astype(float)
                    base = base.drop(columns={'aggregate_x', 'aggregate_y'})
                    base.set_index('time', inplace=True)
                    convert_object2timestamps(base)
                    base.sort_index(inplace=True)
                    return base


        except Exception as e:
            print("Error occured in get_house_data method of IAWE_Loader due to ", e)

    def get_appliance_data(self, appliance: str, houses=None):
        """
        This method will return _EnergyDataset object that can let user access data in dictionary format as well can access some transformation methods

        Parameters
        ----------
        appliance : string
                name of the target appliance (name of the column targeted in the specified house/s)
        house : list
                contains numbers of a specific houses e.g, house=2 for 'house_2.csv'

        returns: _EnergyDataset object
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format
                        {
                            'time': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the specified house

                            *** appliances and their consumption values in numpy.int64 varies house wise ***
                        }
        """
        try:
            self.__data = {}
            if check_correct_datatype(arg_name='appliance', arg_value=appliance, target_datatype=str):
                target_appliance = appliance.lower()
            if houses == None:
                houses = list(self.__keys_of_appliances.keys())

            print(f"Loading data for appliance {target_appliance.upper()} ...")
            if check_list_validations(arg_name='houses', arg_value=houses, member_datatype='int'):
                for house_number in houses:
                    if check_house_availability(arg_name='House Number', arg_value=house_number,
                                                collection=self.__keys_of_appliances.keys()):
                        if target_appliance in [self.__collective_dataset[house_number][i].columns[-1] for i in range(0, len(self.__collective_dataset[house_number]))]:
                            if house_number not in self.__data.keys():
                                print(f"Fetching {target_appliance.upper()} data for House {house_number}")
                                base = self.__collective_dataset[house_number][0]
                                base = base.merge(self.__collective_dataset[house_number][1], how="left", on=["time"])
                                _query = 'appliance=="{}"'.format(target_appliance)
                                base = base.merge(self.__collective_dataset[house_number]
                                                  [self.__keys_of_appliances[house_number].query(_query)['meter'].item()-1], how="left", on=["time"])
                                base = base.compute()
                                base['aggregate_x'][base['aggregate_x'] == "\\N"] = 0.0
                                base['aggregate_y'][base['aggregate_y'] == "\\N"] = 0.0
                                base['aggregate_x'] = base['aggregate_x'].astype('float')
                                base['aggregate_y'] = base['aggregate_y'].astype('float')
                                base['aggregate'] = base['aggregate_x'] + base['aggregate_y']
                                base['aggregate'] = base['aggregate'].astype(float)
                                base[target_appliance] = base[target_appliance].astype(float)
                                base = base.drop(columns={'aggregate_x', 'aggregate_y'})
                                base.set_index('time', inplace=True)
                                convert_object2timestamps(base)
                                base.sort_index(inplace=True)
                                self.__data.update({house_number: base})
                        else:
                            print(f"Appliance '{target_appliance.upper()}' does not exist in house {house_number}.")

            return _EnergyDataset(self.__data)

        except Exception as e:
            print("Error occured in get_appliance_data method of IAWE_Loader due to ", e)

class DEDDIAG_LOADER(CSV_Loader):
    """
    Class that loads all the DEDDIAG data into the memory using DASK and provides different methods to fetch subset of dataset from complete origial.
    """
    def __init__(self, data_path):
        try:
            super().__init__()

        except Exception as e:
            print("Error occured in initialization of DEDDIAG_LOADER class due to ", e)

        finally:
            self.__config = {'DATA_FOLDER': os.path.join(os.getcwd(), data_path), 'DATA_TYPE':'.tsv', 'METADATA': os.path.join(get_module_directory(), 'metadata', 'deddiag'),
                             'DEDDIAG_HOUSES': ['00','01', '02', '03', '04', '05', '06', '07','08', '09', '10', '11', '12', '13', '14']}
            self.__keys_of_appliances = deddiag_parser(_buildings=self.__config['DEDDIAG_HOUSES'], _metadata=self.__config['METADATA'])
            self.__collective_dataset = CSV_Loader._load_files_via_dask_channel_wise(_data_folder=self.__config['DATA_FOLDER'],
                                                                        _metadata=self.__keys_of_appliances,
                                                                _files_format=self.__config['DATA_TYPE'],
                                                                _buildings=self.__config['DEDDIAG_HOUSES'], _dataset='deddiag')

            if self.__collective_dataset:
                print('Dataset successfully loaded!')
            else:
                print('Dataset was not loaded successfully!')


    def get_appliance_names(self, house: int):
        """
        This method will return the names of the available appliances in the specified house number

        Parameters
        ----------
        house : int
                number of a specific house

        returns: list
                 contains names of the available appliances in the specified house
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Fetching appliances for house = {house}")
                    if house == 8:
                        return [name.lower() for name in self.__keys_of_appliances[house]['appliance'][0:5]]
                    else:
                        return [name.lower() for name in self.__keys_of_appliances[house]['appliance']]
        except Exception as e:
            print("Error occured in get_appliance_names method of DEDDIAG_LOADER due to ", e)

    def get_house_data(self, house: int):
        """
        This method will return the dataframe of the specified house number from IAWE dataset

        Parameters
        ----------
        house : int
                number of a specific house/building

        returns: pandas.core.frame.DataFrame
                dataframe is of the following format
                {
                    'time': pandas.core.indexes.datetimes.DatetimeIndex
                        timestamps as index identifying every data row
                    'aggregate': numpy.int64
                        aggregated power consumption of all appliances in the specified house

                    *** appliances and their consumption values in numpy.int64 varies house wise ***
                }
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Loading data for house = {house}")

                    lst_df = []
                    for i in range(0, len(self.__collective_dataset[house])):
                        base = self.__collective_dataset[house][i].compute()
                        base.set_index('time', inplace=True)
                        convert_object2timestamps(base, unit_value='ns')
                        base.sort_index(inplace=True)
                        lst_df.append(base)
                    return lst_df


        except Exception as e:
            print("Error occured in get_house_data method of DEDDIAG_LOADER due to ", e)

    def get_appliance_data(self, appliance: str, houses=None):
        """
        This method will return _EnergyDataset object that can let user access data in dictionary format as well can access some transformation methods

        Parameters
        ----------
        appliance : string
                name of the target appliance (name of the column targeted in the specified house/s)
        house : list
                contains numbers of a specific houses e.g, house=2 for 'house_2.csv'

        returns: _EnergyDataset object
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format
                        {
                            'time': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the specified house

                            *** appliances and their consumption values in numpy.int64 varies house wise ***
                        }
        """
        try:
            self.__data = {}
            if check_correct_datatype(arg_name='appliance', arg_value=appliance, target_datatype=str):
                target_appliance = appliance
            if houses == None:
                houses = list(self.__keys_of_appliances.keys())

            print(f"Loading data for appliance {target_appliance.upper()} ...")
            if check_list_validations(arg_name='houses', arg_value=houses, member_datatype='int'):
                for house_number in houses:
                    if check_house_availability(arg_name='House Number', arg_value=house_number,
                                                collection=self.__keys_of_appliances.keys()):
                        if target_appliance in [self.__collective_dataset[house_number][i].columns[-1] for i in range(0, len(self.__collective_dataset[house_number]))]:
                            if house_number not in self.__data.keys():
                                print(f"Fetching {target_appliance.upper()} data for House {house_number}")
                                _query = 'appliance=="{}"'.format(target_appliance)

                                base = self.__collective_dataset[house_number][self.__keys_of_appliances[house_number][self.__keys_of_appliances[house_number]['appliance']==target_appliance].index.item()]

                                base = base.compute()
                                base.set_index('time', inplace=True)
                                convert_object2timestamps(base, unit_value='ns')
                                base.sort_index(inplace=True)


                                self.__data.update({house_number: base})
                        else:
                            print(f"Appliance '{target_appliance.upper()}' does not exist in house {house_number}.")

            return _EnergyDataset(self.__data)

        except Exception as e:
            print("Error occured in get_appliance_data method of DEDDIAG_LOADER due to ", e)

class GELAP_Loader(CSV_Loader):
    """
    Class that loads all the GELAP data into the memory using DASK and provides different methods to fetch subset of dataset from complete origial.
    """
    def __init__(self, data_path):
        try:
            super().__init__()

        except Exception as e:
            print("Error occured in initialization of GELAP_Loader class due to ", e)

        finally:
            self.__config = {'DATA_FOLDER': os.path.join(os.getcwd(), data_path), 'DATA_TYPE':'.csv', 'METADATA': os.path.join(get_module_directory(), 'metadata', 'gelap'),
                             'GELAP_HOUSES': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11',
                                                '12', '13', '14', '15', '16', '17', '18', '19', '20']}
            self.__keys_of_appliances = gelap_parser(_buildings=self.__config['GELAP_HOUSES'], _metadata=self.__config['METADATA'])
            self.__collective_dataset = CSV_Loader._load_files_via_dask_channel_wise(_data_folder=self.__config['DATA_FOLDER'],
                                                                        _metadata=self.__keys_of_appliances,
                                                                _files_format=self.__config['DATA_TYPE'],
                                                                _buildings=self.__config['GELAP_HOUSES'],
                                                                                     _dataset='gelap')

            if self.__collective_dataset:
                print('Dataset successfully loaded!')
            else:
                print('Dataset was not loaded successfully!')


    def get_appliance_names(self, house: int):
        """
        This method will return the names of the available appliances in the specified house number

        Parameters
        ----------
        house : int
                number of a specific house

        returns: list
                 contains names of the available appliances in the specified house
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Fetching appliances for house = {house}")
                    return [name.lower() for name in self.__keys_of_appliances[house]['appliance']]
        except Exception as e:
            print("Error occured in get_appliance_names method of GELAP_Loader due to ", e)

    def get_house_data(self, house: int):
        """
        This method will return the dataframe of the specified house number from ukdale dataset

        Parameters
        ----------
        house : int
                number of a specific house/building

        returns: pandas.core.frame.DataFrame
                dataframe is of the following format
                {
                    'time': pandas.core.indexes.datetimes.DatetimeIndex
                        timestamps as index identifying every data row
                    'aggregate': numpy.int64
                        aggregated power consumption of all appliances in the specified house

                    *** appliances and their consumption values in numpy.int64 varies house wise ***
                }
        """
        try:
            if check_correct_datatype(arg_name='house', arg_value=house, target_datatype=int):
                if check_house_availability(arg_name='House Number', arg_value=house, collection=self.__keys_of_appliances.keys()):
                    print(f"Loading data for house = {house}")
                    base = self.__collective_dataset[house][0]
                    for i in range(1, len(self.__collective_dataset[house])):
                        base = base.merge(self.__collective_dataset[house][i], how="left", on=["time"])
                    base = base.compute()
                    base.set_index('time', inplace=True)
                    convert_object2timestamps(base, unit_value='ms')
                    base.sort_index(inplace=True)
                    return base

        except Exception as e:
            print("Error occured in get_house_data method of GELAP_Loader due to ", e)

    def get_appliance_data(self, appliance: str, houses=None):
        """
        This method will return _EnergyDataset object that can let user access data in dictionary format as well can access some transformation methods

        Parameters
        ----------
        appliance : string
                name of the target appliance (name of the column targeted in the specified house/s)
        house : list
                contains numbers of a specific houses e.g, house=2 for 'house_2.csv'

        returns: _EnergyDataset object
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format
                        {
                            'time': pandas.core.indexes.datetimes.DatetimeIndex
                                timestamps as index identifying every data row
                            'unix': numpy.int64
                                timestamps in unix
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the specified house

                            *** appliances and their consumption values in numpy.int64 varies house wise ***
                        }
        """
        try:
            self.__data = {}
            if check_correct_datatype(arg_name='appliance', arg_value=appliance, target_datatype=str):
                target_appliance = appliance.lower()
            if houses == None:
                houses = list(self.__keys_of_appliances.keys())

            print(f"Loading data for appliance {target_appliance.upper()} ...")
            if check_list_validations(arg_name='houses', arg_value=houses, member_datatype='int'):
                for house_number in houses:
                    if check_house_availability(arg_name='House Number', arg_value=house_number,
                                                collection=self.__keys_of_appliances.keys()):
                        if target_appliance in [self.__collective_dataset[house_number][i].columns[-1] for i in range(0, len(self.__collective_dataset[house_number]))]:
                            if house_number not in self.__data.keys():
                                print(f"Fetching {target_appliance.upper()} data for House {house_number}")
                                base = self.__collective_dataset[house_number][0]
                                label = self.__keys_of_appliances[house_number][self.__keys_of_appliances[house_number]['appliance'] == target_appliance]['labels'].item()
                                base = base.merge(self.__collective_dataset[house_number][int(label)], how="left", on=["time"])
                                base = base.compute()
                                base.set_index('time', inplace=True)
                                convert_object2timestamps(base, unit_value='ms')
                                base.sort_index(inplace=True)
                                self.__data.update({house_number: base})
                        else:
                            print(f"Appliance '{target_appliance.upper()}' does not exist in house {house_number}.")

            return _EnergyDataset(self.__data)

        except Exception as e:
            print("Error occured in get_appliance_data method of GELAP_Loader due to ", e)


class _EnergyDataset():
    """
    Class that stores the specified data from any data source loader and provides methods to perform transformations
    """
    def __init__(self, data):
        try:
            self.data = data
            self.splits = dict()
            self.activations = dict()
        
        except Exception as e:
            print("Error occured in initialization of _EnergyDataset class due to ", e)
                
        finally:
            pass
    
    def resample(self, sampling_period=None, window_limit=2.0, fill_value=0.0):
        """
        This method will return _EnergyDataset object that can let user access data in dictionary format as well can access some transformation methods

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

        returns: _EnergyDataset object (updated)
                .data = to access data in a dictionary format
                        dictionary contains dataframes of multiple houses where key represents the house number (int) and value represents (pandas.core.frame.DataFrame)
                        dataframe is of the following format            
                        {
                            'time': pandas.core.indexes.datetimes.DatetimeIndex
                                timestamps as index identifying every data row
                            'aggregate': numpy.int64
                                aggregated power consumption of all appliances in the specified house

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
                if self.__sampling_period is None:
                    delta = appliance_data.index[1] - appliance_data.index[0]
                    self.__sampling_period = str(delta.seconds) + 's'
                print(f'sampling_period = {self.__sampling_period}, window_limit = {self.__window_limit} samples, fill_value = {self.__fill_value}\n')
                appliance_data = appliance_data[~appliance_data.index.duplicated(keep='first')]
#                     appliance_data = appliance_data.resample('1s').mean().dropna()
                appliance_data = appliance_data.resample('1s').asfreq()
                appliance_data.fillna(method='ffill', axis=0, inplace=True, limit=self.__window_limit)
                appliance_data.fillna(axis=0, inplace=True, value=self.__fill_value)
                appliance_data = appliance_data.resample(self.__sampling_period).median()
                # appliance_data.dropna(inplace = True)
                self.data.update({house_number: appliance_data})
            print("Updating data with resampled dataset...")

        except Exception as e:
            print("Error occured in resample method of _EnergyDataset due to ", e)
            

    def get_activations(self, target_appliance=None, threshold_x=None, threshold_y=None, min_limit=None, max_limit=None):

        try:
            for house_number, value in self.data.items():
                print(f"Extracting activations for House {house_number}")
                self.activations.update({house_number: get_activities(value, target_appliance, threshold_x, threshold_y, min_limit, max_limit) })
            print("Updating activations with durations when appliance is active (above threshold).")

        except Exception as e:
            print("Error occured in get_activations method of _EnergyDataset due to ", e)


    def subset_data(self, no_of_days=5, threshold = None ):
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
                            aggregated power consumption of all appliances in the specified house
                        *** target_appliance : numpy.int64
                            power consumption of target appliances in the specified house
                    }
        """
        try:
            self.__no_of_days = no_of_days
            for house_number, value in self.data.items():
                print(f"Subetting dataset with {self.__no_of_days} days of most activities for House {house_number}")
                activities = get_activities(value)
                date_wise_activities = activities.groupby([activities['activity_start'].dt.date]).sum()
                time_indices = date_wise_activities.sort_values('duration_in_seconds', ascending=False).head(self.__no_of_days).index
                df_outer = pd.DataFrame()
                for version, time_indx in enumerate(time_indices):
                    df_outer = pd.concat([df_outer, value.loc[str(time_indx)]])
                df_outer.sort_index(inplace=True)
                self.data.update({house_number: df_outer})
            print("Updating data with selected active appliance activities...")

        except Exception as e:
            print("Error occured in subset_data method of _EnergyDataset due to ", e)
            
            
    def get_proportioned_data(self, target_houses, splits_proportion={'TRAIN_PERCENT':0.6, 'VALIDATE_PERCENT':0.2, 'TEST_PERCENT':0.2}):
        """
        """
        try:
            self.__target_houses = target_houses
            self.__proportion = splits_proportion

            print("splits_proportion = ", self.__proportion)

            train_split, val_split, test_split = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

            target_houses['TRAIN'] = sorted(target_houses['TRAIN'])
            target_houses['VALIDATE'] = sorted(target_houses['VALIDATE'])
            target_houses['TEST'] = sorted(target_houses['TEST'])

            lst = [target_houses['TRAIN'], target_houses['VALIDATE'], target_houses['TEST']]
            target_house_lst = [house for house_lst in lst for house in house_lst]
            for house in target_house_lst:
                if house not in self.data.keys():
                    raise Exception('Please only select house that is in the loaded data. OR Load the data of the specific house!')

            if (target_houses['TRAIN'] != target_houses['VALIDATE']) & (target_houses['TRAIN'] == target_houses['TEST']) :
                raise Exception('CASE NOT SUPPORTED: train house is not the same as validation house but train house is same as test house!')

            elif (target_houses['TRAIN'] != target_houses['VALIDATE']) & (target_houses['VALIDATE'] == target_houses['TEST']) :
                raise Exception('CASE NOT SUPPORTED: train house is not the same as validation house but validation house is same as test house!')

            elif (target_houses['TRAIN'] == target_houses['VALIDATE']) & (target_houses['TRAIN'] == target_houses['TEST']) :
                for house_number in self.__target_houses['TRAIN']:
                    df = self.data[house_number]
                    __train_end = df.index[math.floor(self.__proportion['TRAIN_PERCENT'] * len(df))]
                    __val_end = df.index[math.floor((self.__proportion['TRAIN_PERCENT'] + self.__proportion['VALIDATE_PERCENT']) * len(df))]
                    train_split = pd.concat([train_split, df[:__train_end]])
                    val_split = pd.concat([val_split, df[__train_end:__val_end]])
                    test_split = pd.concat([test_split, df[__val_end:]])

            elif (target_houses['TRAIN'] != target_houses['VALIDATE']) & (target_houses['TRAIN'] != target_houses['TEST']):
                for house_number in self.__target_houses['TRAIN']:
                    df = self.data[house_number]
                    __train_end = df.index[math.floor(self.__proportion['TRAIN_PERCENT'] * len(df))]
                    train_split = pd.concat([train_split, df[:__train_end]])
                for house_number in self.__target_houses['VALIDATE']:
                    df = self.data[house_number]
                    __val_end = df.index[math.floor(self.__proportion['VALIDATE_PERCENT'] * len(df))]
                    val_split = pd.concat([val_split, df[:__val_end]])
                for house_number in self.__target_houses['TEST']:
                    df = self.data[house_number]
                    __test_end = df.index[math.floor(self.__proportion['TEST_PERCENT'] * len(df))]
                    test_split = pd.concat([test_split, df[:__test_end]])


            elif (target_houses['TRAIN'] == target_houses['VALIDATE']) & (target_houses['TRAIN'] != target_houses['TEST']):
                for house_number in self.__target_houses['TRAIN']:
                    df = self.data[house_number]
                    __train_end = df.index[math.floor(self.__proportion['TRAIN_PERCENT'] * len(df))]
                    __val_end = df.index[math.floor((self.__proportion['TRAIN_PERCENT'] + self.__proportion['VALIDATE_PERCENT']) * len(df))]
                    train_split = pd.concat([train_split, df[:__train_end]])
                    val_split = pd.concat([val_split, df[__train_end:__val_end]])
                for house_number in self.__target_houses['TEST']:
                    df = self.data[house_number]
                    __test_end = df.index[math.floor(self.__proportion['TEST_PERCENT'] * len(df))]
                    test_split = pd.concat([test_split, df[:__test_end]])

            else:
                pass

            train_split.sort_index(inplace=True)
            val_split.sort_index(inplace=True)
            test_split.sort_index(inplace=True)
            self.splits.update({'TRAIN': train_split, 'VALIDATE': val_split, 'TEST': test_split})
            print("Updating splits with specified proportion from every target house...")

        except Exception as e:
            print("Error occured in get_proportioned_data method of _EnergyDataset due to ", e)


    def save_splits(self, output_directory):
        """
        """
        try:
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)

            self.splits['TRAIN'].to_csv(os.path.join(output_directory, 'train.csv'))
            self.splits['VALIDATE'].to_csv(os.path.join(output_directory, 'validation.csv'))
            self.splits['TEST'].to_csv(os.path.join(output_directory, 'test.csv'))

        except Exception as e:
            print("Error occured in save_splits method due to ", e)


    def normalize(self, scaler = 'Standard', scalars_directory='scalers/', training=False ):
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
            if scaler == 'Standard' or scaler == 'standard':
                X_scaler, y_scaler = StandardScaler(), StandardScaler()

            elif scaler == 'MinMax' or scaler == 'minmax':
                X_scaler, y_scaler = MinMaxScaler(), MinMaxScaler()

            elif scaler == 'Robust' or scaler == 'robust':
                X_scaler, y_scaler = RobustScaler(), RobustScaler()

            else:
                print('Argument \'scaler\' value is undefined! Setting it to standard scaling....')
                X_scaler, y_scaler = StandardScaler(), StandardScaler()

            if not os.path.exists(scalars_directory):
                os.makedirs(f'{scalars_directory}/', )  # if not then create folder
            print(f"Saving scalers to directory {scalars_directory}")

            if not training:
                for house_number in self.data.keys():
                    print(f"Normalizing for house number: ", house_number)
                    __appliance_data = self.data[house_number]
                    if len(__appliance_data.columns)>1:
                        X_array = __appliance_data['aggregate'].values
                        X_scaler.fit(X_array.reshape(-1, 1))
                        pickle.dump(X_scaler, open(os.path.join(scalars_directory, f'X_scaler_house_{house_number}.sav'), 'wb'))
                        X = X_scaler.transform(X_array.reshape(-1, 1)).flatten()

                    if __appliance_data.columns[-1] != 'aggregate':
                        target_appliance = __appliance_data.columns[-1]
                    else:
                        target_appliance = __appliance_data.columns[0]
                    y_array = __appliance_data[target_appliance].values
                    y_scaler.fit(y_array.reshape(-1, 1))
                    pickle.dump(y_scaler, open(os.path.join(scalars_directory, f'y_scaler_house_{house_number}.sav'), 'wb'))
                    y = y_scaler.transform(y_array.reshape(-1, 1)).flatten()

                    if len(__appliance_data.columns)>1:
                        __normalized_df = pd.DataFrame({'time': __appliance_data.index, 'aggregate': X, f"{target_appliance}":y}).set_index('time')
                    else:
                        __normalized_df = pd.DataFrame({'time': __appliance_data.index, f"{target_appliance}":y}).set_index('time')
                    self.data.update({house_number: __normalized_df})
                print("Updating data with normalized dataset...")

            else:
                print('Normalization is being performed for training a model. Scalers will be computed/fit considering the train_split and using those scalers, all splits will be normalized/transformed.')

                if len(self.splits) == 0:
                    raise Exception("Call the function get_proportioned_data to specify the training, validation and testing splits, in order to perform normalization for training.")
                else:
                    if self.splits['TRAIN'].columns[-1] != 'aggregate':
                        target_appliance = self.splits['TRAIN'].columns[-1]
                    else:
                        target_appliance = self.splits['TRAIN'].columns[0]

                    if 'aggregate' not in self.splits['TRAIN'].columns:
                        raise Exception('Splits cannot be used for NILM training. "aggregate" is not available!')

                    X_array = self.splits['TRAIN']['aggregate'].values
                    y_array = self.splits['TRAIN'][target_appliance].values

                    X_scaler.fit(X_array.reshape(-1, 1))
                    y_scaler.fit(y_array.reshape(-1, 1))

                    pickle.dump(X_scaler, open(os.path.join(scalars_directory, f'X_scaler.sav'), 'wb'))
                    pickle.dump(y_scaler, open(os.path.join(scalars_directory, f'y_scaler.sav'), 'wb'))

                    X_train = X_scaler.transform(X_array.reshape(-1, 1)).flatten()
                    y_train = y_scaler.transform(y_array.reshape(-1, 1)).flatten()

                    __train_df = pd.DataFrame({'time': self.splits['TRAIN'].index, 'aggregate': X_train,
                                               f"{target_appliance}": y_train}).set_index('time')

                    X_array = self.splits['VALIDATE']['aggregate'].values
                    y_array = self.splits['VALIDATE'][target_appliance].values

                    X_val = X_scaler.transform(X_array.reshape(-1, 1)).flatten()
                    y_val = y_scaler.transform(y_array.reshape(-1, 1)).flatten()

                    X_array = self.splits['TEST']['aggregate'].values
                    y_array = self.splits['TEST'][target_appliance].values

                    X_test = X_scaler.transform(X_array.reshape(-1, 1)).flatten()
                    y_test = y_scaler.transform(y_array.reshape(-1, 1)).flatten()

                    __val_df = pd.DataFrame({'time': self.splits['VALIDATE'].index, 'aggregate': X_val, f"{target_appliance}": y_val}).set_index('time')
                    __test_df = pd.DataFrame({'time': self.splits['TEST'].index, 'aggregate': X_test,f"{target_appliance}": y_test}).set_index('time')

                    print("Updating splits with normalized data splits...")
                    self.splits = {'TRAIN': __train_df, 'VALIDATE': __val_df, 'TEST': __test_df}

        except Exception as e:
            print("Error occured in normalize method of _EnergyDataset due to ", e)

