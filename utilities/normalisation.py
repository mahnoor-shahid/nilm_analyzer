
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import numpy as np
import os, pickle


def normalize(df, train_flag, scalars_directory ):
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
        X_scaler, y_scaler = StandardScaler(), StandardScaler()
        
        if not os.path.exists(scalars_directory):
            os.makedirs(f'{scalars_directory}/', )  # if not then create folder
        
        if train_flag:
            
            df[df.columns[0]] = X_scaler.fit_transform(df[df.columns[0]].values.reshape(-1, 1)).squeeze()
            df[df.columns[-1]] = y_scaler.fit_transform(df[df.columns[0]].values.reshape(-1, 1)).squeeze()
            
            pickle.dump(X_scaler, open(os.path.join(scalars_directory, 'X_scaler.sav'), 'wb'))
            pickle.dump(y_scaler, open(os.path.join(scalars_directory, 'y_scaler.sav'), 'wb'))
            
        else:
            
            X_scaler = pickle.load(open(os.path.join(scalars_directory, 'X_scaler.sav'), 'rb'))
            y_scaler = pickle.load(open(os.path.join(scalars_directory, 'y_scaler.sav'), 'rb'))
            
            df[df.columns[0]] = X_scaler.transform(df[df.columns[0]].values.reshape(-1, 1)).squeeze()
            df[df.columns[-1]] = y_scaler.transform(df[df.columns[0]].values.reshape(-1, 1)).squeeze()
        
        return df

    except Exception as e:
        print("Error occured in normalize method of REFIT_Loader utilities due to ", e) 
