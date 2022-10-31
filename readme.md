# REFIT Loader
> This project uses **Dask Dataframes** to ease and fasten the process of loading all the data of REFIT and provides functionalities to transform and manipulate the REFIT dataset for statistical analysis purpose.


## REFIT dataset
An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study. Sci Data 4, 160122 (2017). <br />
Murray, D., Stankovic, L. & Stankovic, V.  <br />


### Links
For more detail information, visit the following links: <br />
http://dx.doi.org/10.1038/sdata.2016.122 <br />
https://rdcu.be/cMD9F <br />


## Steps to implement this project
1) Clone this repository to the target source project.
```
git clone https://github.com/mahnoor-shahid/refit_loader.git
```
*You can skip Step 2 and 3 if all the [dependencies](#dependencies) are already in installed in the current environment*

2) Create a conda environment using the environment.yml file <br/>
```
cd refit_loader/
conda create env --file=environment.yml
```

3) Activate the created environment
```
conda activate refit_loader_env
```

4) Download the REFIT dataset in the **data/** folder (*it might take some time dataset takes the storage of around 6GB*)
```
cd data/
Invoke-WebRequest https://pureportal.strath.ac.uk/files/52873459/Processed_Data_CSV.7z -O Processed_Data_CSV.7z
```
5) Unzip the downloaded REFIT dataset 
```
unzip Processed_Data_CSV.7z -d refit/
```
6) Make sure to download the "REFIT_Readme.txt" and save it in the **data/refit/** folder
```
cd refit_loader/data/refit
Invoke-WebRequest https://pureportal.strath.ac.uk/files/52873458/REFIT_Readme.txt -O REFIT_Readme.txt
```
7) Use the notebooks "geting_started.ipynb" and "resampling.ipynb" to know the instructions on how to use the loader


### Repo Structure:
This repository follows the below structure format:
```
|
|── loader
|  └── __init__.py
|  └── data_loader.py
|
|
├── utils
|  └── configuration.py
|  └── parser.py
|  └── time_utils.py
|  └── validations.py
|
|
├── data
|  └── refit
|  |  └── REFIT_Readme.txt
|  |  └── House_1.csv
|  |  └── House_2.csv
|  |  └── House_3.csv
|  |  └── House_4.csv
|  |  └── House_5.csv
|  |  └── House_6.csv
|  |  └── House_7.csv
|  |  └── House_8.csv
|  |  └── House_9.csv
|  |  └── House_10.csv
|  |  └── House_11.csv
|  |  └── House_12.csv
|  |  └── House_13.csv
|  |  └── House_15.csv
|  |  └── House_16.csv
|  |  └── House_17.csv
|  |  └── House_18.csv
|  |  └── House_19.csv
|  |  └── House_20.csv
|
|
├── config.json
|
├── 01_getting_started.ipynb
|
├── 02_resampling.ipynb
|
├── environment.yml
|
├── readme.md
```



## Downloads
The REFIT Smart Home dataset is a publicly available dataset of Smart Home data. <br />
Dataset - https://pureportal.strath.ac.uk/files/52873459/Processed_Data_CSV.7z <br />
Readme File - https://pureportal.strath.ac.uk/files/52873458/REFIT_Readme.txt <br />
Main Page - https://pureportal.strath.ac.uk/en/datasets/refit-electrical-load-measurements-cleaned


## Dependencies
```
  - python=3.9.2
  - numpy=1.20.3
  - pandas=1.2.4
  - dask=2021.06.2
  - json=2.0.9
```

## Citation
```
Murray, D., Stankovic, L. & Stankovic, V. An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study. Sci Data 4, 160122 (2017). https://doi.org/10.1038/sdata.2016.122
```

