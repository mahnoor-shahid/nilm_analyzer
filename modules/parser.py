

import yaml
from yaml.loader import SafeLoader
import json
import os
import pandas as pd

def refit_parser(_buildings, _metadata):
    """
    This method will return the dictionary of the appliances, parsed from README file of refit data

    Parameters 
    ----------
    readme_file : txt file 
                    original README file of refit datase

    returns: pandas.Dictionary
                    dictionary contains the all the appliances in a specific house as mentioned in README file
    """
    try:
        print(f'Fetching the meter_appliance mapping from REFIT metadata: {_metadata}')

        building_metadata = {}
        for building in _buildings:
            building_path = os.path.join(_metadata, f'building{building}.yaml')
            with open(building_path) as f:
                data = yaml.load(f, Loader=SafeLoader)

                meters, appliances = [], []
                meter_app_mapping = {}
                for app_dict in data['appliances']:
                    if 'meters' in app_dict.keys():
                        if 'type' in app_dict.keys():
                            meters.append(app_dict['meters'][0] - 1)
                            appliances.append(app_dict['type'])

                meter_app_mapping.update({'meter': meters, 'appliance': appliances})
            building_metadata.update({building: pd.DataFrame(meter_app_mapping).drop_duplicates().sort_values(by='meter')})
        return building_metadata

    except Exception as e:
        print("Error occured in refit_parser method due to ", e)



def ukdale_parser(_buildings, _metadata):

    try:
        print(f'Fetching the meter_appliance mapping from UKDALE metadata: {_metadata}')

        building_metadata = {}
        for building in _buildings:
            building_path = os.path.join(_metadata, f'building{building}.yaml')
            with open(building_path) as f:
                data = yaml.load(f, Loader=SafeLoader)
                meters, appliances = [1], ['aggregate']
                meter_app_mapping = {}
                for app_dict in data['appliances']:
                    if 'meters' in app_dict.keys():
                        if 'original_name' in app_dict.keys():
                            meters.append(app_dict['meters'][0])
                            appliances.append(app_dict['original_name'])

                meter_app_mapping.update({'meter': meters, 'appliance': appliances})
            building_metadata.update({building: pd.DataFrame(meter_app_mapping).drop_duplicates().sort_values(by='meter')})
        return building_metadata

    except Exception as e:
        print("Error occured in ukdale_parser method due to ", e)


def ampds_parser(_buildings, _metadata):

    try:
        print(f'Fetching the meter_appliance mapping from AMPDS metadata: {_metadata}')

        building_metadata = {}
        for building in _buildings:
            building_path = os.path.join(_metadata, f'building{building}.yaml')
            with open(building_path) as f:
                data = yaml.load(f, Loader=SafeLoader)
                meters, appliances = [], []
                meter_app_mapping = {}
                for app_dict in data['appliances']:
                    if 'original_name' in app_dict.keys():
                        if 'type' in app_dict.keys():
                            meters.append(app_dict['original_name'])
                            if app_dict['type'] == 'unknown':
                                appliances.append(app_dict['original_name'])
                            else:
                                appliances.append(app_dict['type'])

                meter_app_mapping.update({'meter': meters, 'appliance': appliances})
            building_metadata.update({building: pd.DataFrame(meter_app_mapping).drop_duplicates().sort_values(by='meter')})
        return building_metadata

    except Exception as e:
        print("Error occured in ampds_parser method due to ", e)


def iawe_parser(_buildings, _metadata):

    try:
        print(f'Fetching the meter_appliance mapping from IAWE metadata: {_metadata}')

        building_metadata = {}
        for building in _buildings:
            building_path = os.path.join(_metadata, f'building{building}.yaml')
            with open(building_path) as f:
                data = yaml.load(f, Loader=SafeLoader)
                meters, appliances = [1, 2], ['aggregate', 'aggregate']
                meter_app_mapping = {}
                for app_dict in data['appliances']:
                    if 'meters' in app_dict.keys():
                        if 'original_name' in app_dict.keys():
                            meters.append(app_dict['meters'][0])
                            appliances.append(app_dict['original_name'])

                meter_app_mapping.update({'meter': meters, 'appliance': appliances})
            building_metadata.update(
                {building: pd.DataFrame(meter_app_mapping).drop_duplicates().sort_values(by='meter')})
        return building_metadata

    except Exception as e:
        print("Error occured in iawe_parser method due to ", e)


def deddiag_parser(_buildings, _metadata):

    try:
        print(f'Fetching the meter_appliance mapping from DEDDIAG metadata: {_metadata}')

        building_metadata = {}
        for building in _buildings:
            building_path = os.path.join(_metadata, f'house_{building}.tsv')
            data = pd.read_csv(building_path, sep='\t')
            meter_app_mapping = {}
            if 'category' in data.keys():
                if 'item_id' in data.keys():
                    meters = list(data['item_id'])
                    appliances = list(data['category'])
                meter_app_mapping.update({'meter': meters, 'appliance': appliances})
            building_metadata.update({int(building): pd.DataFrame(meter_app_mapping).drop_duplicates().sort_values(by='meter')})
        return building_metadata

    except Exception as e:
        print("Error occured in deddiag_parser method due to ", e)


def gelap_parser(_buildings, _metadata):

    try:
        print(f'Fetching the meter_appliance mapping from GELAP metadata: {_metadata}')

        building_metadata = {}
        for building in _buildings:
            building_path = os.path.join(_metadata, f'hh-{building}', 'info.json')

            with open(building_path) as user_file:
                file_contents = json.load(user_file)

            meter_app_mapping = {}
            labels, appliances = [], []
            for value in file_contents['label_data']:
                labels.append(value)
                appliances.append(file_contents['label_data'][value]['type'])
                meter_app_mapping.update({'labels': labels, 'appliance': appliances})
            building_metadata.update(
                {int(building): pd.DataFrame(meter_app_mapping).drop_duplicates().sort_values(by='labels')})
        return building_metadata

    except Exception as e:
        print("Error occured in gelap_parser method due to ", e)