
# NILM_Analyzer: A Simple Python Package
> This project uses **Dask Dataframes** to ease and fasten the process of loading and analyzing all the data of any publicly available NILM dataset and provides basic transformations like resampling, standardization and extracting activations by thresholding for statistical analysis purpose. Can be used further for splitting datasets into train, validation and test subsets for Energy Disaggregation task. 

## Getting Started
1) Install the nilm_analyzer in your current environment.
```
pip install nilm_analyzer
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
[NILM Analyzer Tutorials](https://github.com/mahnoor-shahid/nilm_analyzer_tutorials) = This repository serves more like a user guide that describes how to use the nilm analyzer package, and demonstrates all the basic functionalities that it provides.


## Dependencies
Ensure that the following dependencies are satisfied in your current environment 
```  
  - python>=3.9.2
  - numpy>=1.20.3
  - pandas>=1.2.4
  - dask>=2021.06.2
  - scikit-learn>=1.1.2
```


## Datasets Included
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


## Downloads
REFIT [UK]
https://pureportal.strath.ac.uk/files/52873459/Processed_Data_CSV.7z

UK-DALE [UK]
http://data.ukedc.rl.ac.uk/simplebrowse/edc/efficiency/residential/EnergyConsumption/Domestic/UK-DALE-2017/UK-DALE-FULL-disaggregated/ukdale.zip

AMPds [Canada]
https://dataverse.harvard.edu/api/access/datafile/2741425?format=original

GeLaP [GERMANY]
https://mygit.th-deg.de/tcg/gelap/-/tree/master

DEDDIAG [GERMANY]
https://figshare.com/articles/dataset/DEDDIAG_a_domestic_electricity_demand_dataset_of_individual_appliances_in_Germany/13615073

iAWE [INIDA]
https://drive.google.com/open?id=1c4Q9iusYbwXkCppXTsak5oZZYHfXPmnp


