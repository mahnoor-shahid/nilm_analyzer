
import json

def get_config_from_json(description, config_file):
    """
    This method will return the dictionary of the specified config_file

    Parameters 
    ----------
    description : string 
                    description of the json file     
    config_file : string
                    path of the configuration file to load
                    
    returns: pandas.Dictionary
                    dictionary contains the all the properties set for the configuration which can be accessed with keys
    """
    with open(config_file, 'r') as json_file:
        try:
            config_dict = json.load(json_file)
            print(f"Followings are the {description} ")
            print(config_dict)
            return config_dict
        
        except ValueError:
            print("Invalid JSON file format or JSON config file location. Please provide the correct location of the targeted json file")
            exit(-1)