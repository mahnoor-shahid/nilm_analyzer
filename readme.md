# REFIT Loader
> This project uses **Dask Dataframes** to ease and fasten the process of loading all the data of REFIT and provides functionalities to transform and manipulate the REFIT dataset for statistical analysis purpose.


### About REFIT dataset
An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study. Sci Data 4, 160122 (2017). <br />
Murray, D., Stankovic, L. & Stankovic, V.  <br />

For more detail information, visit the following links: <br />
http://dx.doi.org/10.1038/sdata.2016.122 <br />
https://rdcu.be/cMD9F <br />

## Dependencies
Ensure that the following dependencies are satisfied either in your current environment 
```  
  - python>=3.9.2
  - numpy>=1.20.3
  - pandas>=1.2.4
  - dask>=2021.06.2
  - scikit-learn>=1.1.2
```
or create a new environment using 'environment.yml'
```
conda create env --file=environment.yml
conda activate refit_loader_env
```


## Steps to implement this project
1) Install the refit-loader in your current target environment
```
pip install refit-loader
```

2) [Download](#downloads) the refit dataset. Import the REFIT_Loader and pass the data path to the refit object.
```
from refit_loader.data_loader import REFIT_Loader
refit = REFIT_Loader(data_path='')
```
3) Fetch data using the created refit object.
```
kettle = refit.
```

4) Take the reference from Refit_Analyzer to see how Refit_Loader can be accessed and how it's utilities can be used.

Reference Repository: <br />
[Refit Analyzer](https://github.com/mahnoor-shahid/refit_analyzer) = REFIT analyzer is more like a user guide that uses REFIT Loader as a submodule and demonstrates how it can be used and how it can be useful with its utilities.


## Repo Structure:
This repository follows the below structure format:
```
|
|── data_loader.py
|
├── metadata
|  └── REFIT_README.txt
|
├── modules
|  └── active_durations.py
|  └── parser.py
|  └── validations.py
|  └── normalisation.py
|
├── environment.yml
|
├── readme.md
|
```

## Downloads
The REFIT Smart Home dataset is a publicly available dataset of Smart Home data. <br />
Dataset - https://pureportal.strath.ac.uk/files/52873459/Processed_Data_CSV.7z <br />
Main Page - https://pureportal.strath.ac.uk/en/datasets/refit-electrical-load-measurements-cleaned

## Citation
```
Murray, D., Stankovic, L. & Stankovic, V. An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study. Sci Data 4, 160122 (2017). https://doi.org/10.1038/sdata.2016.122
```

