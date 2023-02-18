# nilm_datasets package
> This project uses **Dask Dataframes** to ease and fasten the process of loading and analyzing all the data of any publicly available NILM dataset and provides basic transformations like resampling, standardization and extracting activations by thresholding for statistical analysis purpose. Can be used further for splitting datasets into train, validation and test subsets for Energy Disaggregation task. 


## Getting Started
1) Install the refit-loader in your current target environment.
```
pip install nilm_datasets
```

2) [Download](#downloads) any NILM dataset(s) and import the corresponding loader. Then, pass the data path of the data directory where the dataset is located. For instance,
```
from nilm_datasets.loaders import REFIT_Loader
refit = REFIT_Loader(data_path='data/refit/')
```
3) Fetch the list of available appliances for selected houses.
```
refit.get_appliance_names(house=2)
```
4) Load data for selected appliance (all houses)
```
kettle = refit.get_appliance_data(appliance='Kettle')
```
5) (OR) Load data for selected house (all appliances).
```
house2 = refit.get_house_data(house=2)
```
6) (OR) Load data for sselected appliance and elected houses.
```
kettle = refit.get_appliance_data(appliance="Kettle", houses=[1,2,3])
```
7) Take the reference from NILM_Analyzer to see how Refit_Loader can be accessed and how it's utilities can be used.

Reference Repository: <br />
[NILM Analyzer](https://github.com/mahnoor-shahid/nilm_analyzer) = NILM analyzer is more like a user guide that explains how nilm_datasets package can be utilized and explains how they can be analyzed using nilm_datasets package and demonstrates all the basic functionalities that it provides.
[Refit Analyzer](https://github.com/mahnoor-shahid/refit_analyzer) = REFIT analyzer is more like a user guide that uses nilm_datasets can be accessed and how to utilize its 

## Dependencies
Ensure that the following dependencies are satisfied in your current environment 
```  
  - python>=3.9.2
  - numpy>=1.20.3
  - pandas>=1.2.4
  - dask>=2021.06.2
  - scikit-learn>=1.1.2
```


### Datasets Included
REFIT [United Kingdom] <br />
Murray, D., Stankovic, L. & Stankovic, V. An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study. Sci Data 4, 160122 (2017). https://doi.org/10.1038/sdata.2016.122 <br />

UK-DALE [United Kingdom] <br />
Kelly, J., Knottenbelt, W. The UK-DALE dataset, domestic appliance-level electricity demand and whole-house demand from five UK homes. Sci Data 2, 150007 (2015). https://doi.org/10.1038/sdata.2015.7 <br />

GeLaP [Germany] <br />
Wilhelm, S., Jakob, D., Kasbauer, J., Ahrens, D. (2022). GeLaP: German Labeled Dataset for Power Consumption. In: Yang, XS., Sherratt, S., Dey, N., Joshi, A. (eds) Proceedings of Sixth International Congress on Information and Communication Technology. Lecture Notes in Networks and Systems, vol 235. Springer, Singapore. https://doi.org/10.1007/978-981-16-2377-6_5 <br />

DEDDIAG [Germany] <br />
Wenninger, M., Maier, A. & Schmidt, J. DEDDIAG, a domestic electricity demand dataset of individual appliances in Germany. Sci Data 8, 176 (2021). https://doi.org/10.1038/s41597-021-00963-2 <br />

AMPds [Canada] <br />
S. Makonin, F. Popowich, L. Bartram, B. Gill and I. V. Bajić, "AMPds: A public dataset for load disaggregation and eco-feedback research," 2013 IEEE Electrical Power & Energy Conference, Halifax, NS, Canada, 2013, pp. 1-6, doi: 10.1109/EPEC.2013.6802949. <br />

iAWE [India] <br />
N. Batra, A. Singh, P. Singh, H. Dutta, V. Sarangan, M. Srivastava "Data Driven Energy Efficiency in Buildings"



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

