# REFIT Loader
> This project uses **Dask Dataframes** to ease and fasten the process of loading all the data of REFIT and provides functionalities to transform and manipulate the REFIT dataset for statistical analysis purpose.


## REFIT dataset
An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study. Sci Data 4, 160122 (2017). <br />
Murray, D., Stankovic, L. & Stankovic, V.  <br />

### Links
For more detail information, visit the following links: <br />
http://dx.doi.org/10.1038/sdata.2016.122 <br />
https://rdcu.be/cMD9F <br />



## Dependencies
Ensure that the following dependencies are satisfied either in your current environment 
```
  - python=3.9.2
  - numpy=1.20.3
  - pandas=1.2.4
  - dask=2021.06.2
  - json=2.0.9
```
or create a new environment using 'environment.yml'
```
conda create env --file=environment.yml
conda activate refit_loader_env
```


## Steps to implement this project
1) Use this repository as a submodule and clone it into your target source project
```
git submodule add https://github.com/mahnoor-shahid/refit_loader.git
```

2) Make sure the 'config.json' file has the correct DATA_FOLDER path; [Download](## From where to download the dataset ) the dataset and it should be located in this data folder.
```
{ 
    "DATA_FOLDER" : "data/refit/",
    "DATA_TYPE" : ".csv",
    "README_FILE" : "refit_loader/REFIT_Readme.txt",
    "REFIT_HOUSES" : [1,2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,20,21]
}
```

3) Take the reference from Refit_Analyzer to see how Refit_Loader can be accessed as a submodule and how it's utilities can be used.

Reference Repository: <br />
[Refit Analyzer](https://github.com/mahnoor-shahid/refit_analyzer) = REFIT analyzer is more like a user guide that uses REFIT Loader as a submodule and demonstrates how it can be used and how it can be useful with its utilities.


## Repo Structure:
This repository follows the below structure format:
```
|
|── data_loader.py
|
├── utilities
|  └── active_durations.py
|  └── configuration.py
|  └── parser.py
|  └── time_utils.py
|  └── validations.py
|
├── config.json
|
├── environment.yml
|
├── REFIT_README.txt
|
├── readme.md
|
```

## From where to download the dataset 
The REFIT Smart Home dataset is a publicly available dataset of Smart Home data. <br />
Dataset - https://pureportal.strath.ac.uk/files/52873459/Processed_Data_CSV.7z <br />
Main Page - https://pureportal.strath.ac.uk/en/datasets/refit-electrical-load-measurements-cleaned


## Citation
```
Murray, D., Stankovic, L. & Stankovic, V. An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study. Sci Data 4, 160122 (2017). https://doi.org/10.1038/sdata.2016.122
```

