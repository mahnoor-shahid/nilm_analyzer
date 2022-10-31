
def refit_parser(readme_file):
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
        print(f'Parsing the readme file specified: {readme_file}')
        with open(readme_file) as f:
            content = f.readlines()
        ls = {}
        for i, s in enumerate(content):
            if 'House' in s.capitalize():
                keys, appliances = [], []
                house = s.split()[1]
                for indx in range(1, 6):
                    if content[i+indx] == '\t!NOTES\n':
                        break
                    else:
                        target = [value.split('.') for value in [value for value in content[i+indx].split(',') if value != '\n']]
                        for t in target:
                            if len(t) > 2: ##### one comma missing in house 5 caused issue
                                appliances.append(t[1])
                                appliances.append(t[2])
                            else:
                                appliances.append(t[1])
                ls.update({house: [item.split('\n')[0].replace(" ", "") for item in appliances]})
        return ls
    
    except Exception as e:
        print("Error occured in parser method due to ", e)