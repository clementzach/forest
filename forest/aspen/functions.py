import os
import glob
import pandas as pd
import numpy as np

####contains utility functions which the user is less likely to see


import glob
def read_and_aggregate(download_folder, beiwe_id, data_stream, tz_str = "UTC"):
    """
    Reads in all downloaded data for a particular user and data stream and stacks the datasets

    Args:
        download_folder (str):
            path to downloaded data. This is a folder that includes the user data in a subfolder with the beiwe_id as the subfolder name
        beiwe_id (str):
            ID of user to aggregate data
        data_stream (str):
            Data stream to aggregate. Must be a datastream name as downloaded from the server 
        tz_str (str): 
            Time Zone to include in local_time column of output. See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones for options

    Returns:
        aggregated_data (DataFrame): dataframe with stacked data, a field for the beiwe ID, a field for the day of week.
    """
    #Note: this is similar to a function in sycamore. Sycamore may be able to use this one, but this one couldn't use the one from sycamore. 
    st_path = os.path.join(download_folder, beiwe_id, data_stream)
    if os.path.isdir(st_path):
        # get all csv files in immediate or subdirectories
        all_files = glob.glob(os.path.join(st_path, '**/*.csv'), recursive=True)
        # Sort file paths for when they're read in
        all_files = sorted(all_files)
        if len(all_files) > 0:
            # Read in all files
            aggregated_data = [pd.read_csv(file) for file in all_files]
            aggregated_data = pd.concat(aggregated_data, axis=0, ignore_index = True)
            aggregated_data['beiwe_id'] = beiwe_id
            aggregated_data['UTC time'] = pd.to_datetime(aggregated_data['UTC time'])
            aggregated_data['local_time'] = aggregated_data['UTC time'].dt.tz_localize('UTC').dt.tz_convert(tz_str)
            aggregated_data['date'] = aggregated_data['local_time'].dt.date

            return(aggregated_data)
        

    print(f'No {data_stream} csv data for user {beiwe_id}')
    return(pd.DataFrame(columns = ['beiwe_id', 'date', 'UTC time', 'local_time'])) #blank df
    
    
def count_files(download_folder, beiwe_id, data_stream, tz_str = "UTC"):
    '''
    Counts the number of files in a data stream per day
    
    Args:
        download_folder (str):
            path to downloaded data. This is a folder that includes the user data in a subfolder with the beiwe_id as the subfolder name
        beiwe_id (str):
            ID of user of which to count files
        data_stream (str):
            Data stream to aggregate. Must be a datastream name as downloaded from the server 
        tz_str (str): 
            Time Zone to use to define days and to be used in date column of output
    Returns:
        counts_df (DataFrame): dataframe with stacked data, a field for the beiwe ID, a field for the day of week.
        
    '''
    st_path = os.path.join(download_folder, beiwe_id, data_stream)
    if os.path.isdir(st_path):
        all_files = [file.split('.')[0]  for root, dirs, files in os.walk(st_path) for file in files if file.split('.')[0] != '']
        if len(all_files) > 0:
            dates_df = pd.DataFrame(all_files, columns = ['utc_time'])
            dates_df.utc_time = dates_df.utc_time.apply(lambda x: x.replace("_", ":"))

            dates_df.utc_time = pd.to_datetime(dates_df.utc_time)
            if dates_df.dtypes['utc_time'] == "datetime64[ns]": ## Not timezone localized
                dates_df['utc_time'] = dates_df['utc_time'].dt.tz_localize('UTC') 
                #assume they're from UTC if no TZ info included in filename

            dates_df['local_time'] = dates_df.utc_time.dt.tz_convert(tz_str)
            dates_df['day'] = dates_df.local_time.dt.date
            counts_df = pd.DataFrame(dates_df.day.value_counts()).reset_index()

            counts_df.rename({'day': data_stream + '_file_count', 'index': 'date'}, axis = 1,inplace = True)
            return(counts_df)

    counts_df = pd.DataFrame({data_stream + '_file_count': [], 'date' : []}) #blank df with same cols
    counts_df['date'] = counts_df['date'].astype('datetime64[ns]')
    return(counts_df)


def file_size(download_folder, beiwe_id, data_stream, tz_str="UTC"):
    '''
    Counts the number of files in a data stream per day

    Args:
        download_folder (str):
            path to downloaded data. This is a folder that includes the user data in a subfolder with the beiwe_id as the subfolder name
        beiwe_id (str):
            ID of user of which to count files
        data_stream (str):
            Data stream to aggregate. Must be a datastream name as downloaded from the server
        tz_str (str):
            Time Zone to use to define days and to be used in date column of output
    Returns:
        counts_df (DataFrame): dataframe with stacked data, a field for the beiwe ID, a field for the day of week.

    '''
    st_path = os.path.join(download_folder, beiwe_id, data_stream)
    if os.path.isdir(st_path):
        all_dates = [file.split('.')[0] for root, dirs, files in
                     os.walk(st_path) for file in files if
                     file.split('.')[0] != '']

        all_sizes = [os.path.getsize(os.path.join(st_path,file)) for root, dirs, files in
                     os.walk(st_path) for file in files if
                     file.split('.')[0] != '']
        if len(all_dates) > 0:
            dates_df = pd.DataFrame({'utc_time': all_dates, 'file_size':all_sizes})
            dates_df.utc_time = dates_df.utc_time.apply(
                lambda x: x.replace("_", ":"))

            dates_df.utc_time = pd.to_datetime(dates_df.utc_time)
            if dates_df.dtypes[
                'utc_time'] == "datetime64[ns]":  ## Not timezone localized
                dates_df['utc_time'] = dates_df['utc_time'].dt.tz_localize(
                    'UTC')
                # assume they're from UTC if no TZ info included in filename

            dates_df['local_time'] = dates_df.utc_time.dt.tz_convert(tz_str)
            dates_df['day'] = dates_df.local_time.dt.date
            size_df = dates_df.groupby('day').file_size.agg('sum').reset_index()

            size_df.rename(
                {'file_size': data_stream + '_file_size', 'day' : 'date'}, axis=1,
                inplace=True)
            return (size_df)

    size_df = pd.DataFrame({data_stream + '_file_size': [],
                              'date': []})  # blank df with same cols
    size_df['date'] = size_df['date'].astype('datetime64[ns]')
    return (size_df)
        
        
def get_count_per_day(aggregated_data, prefix_str):
    """
    Reads in an aggregated dataframe and counts number of observations for a given day
    
    Args:
        aggregated_data (DataFrame): a user's aggregated study data

        prefix_str (str): the prefix to append to column names (like 'accelerometer' or 'gps')
    returns:
        obs_time_df (DataFrame): a dataframe with a column of dates and a column of observation time per date
        
    """
    if aggregated_data.shape[0] == 0:
        blank_df = pd.DataFrame(columns = ['date', prefix_str + '_line_count', prefix_str + '_any_data'])
        blank_df['date'] = blank_df['date'].astype('datetime64[ns]')
        return(blank_df)
        
    #sort just to make sure...
    aggregated_data.sort_values('local_time', inplace = True, ascending = True)
    min_day = np.min(aggregated_data['date'])
    max_day = np.max(aggregated_data['date'])
    date = min_day

    obs_counts_df_list = [] 
    while date <= max_day:
      #grab all lines in that day
        day_df = aggregated_data.loc[aggregated_data['local_time'].dt.date == date, : ]
        temp_df = pd.DataFrame({'date': [date], 'count': [day_df.shape[0]], 'any_data': [int(day_df.shape[0] > 0)]})

        obs_counts_df_list.append(temp_df)
        date = date + pd.Timedelta(1,unit = 'day') #increment to next day
    obs_counts_df = pd.concat(obs_counts_df_list, axis = 0, ignore_index = True) #bind dfs together
    obs_counts_df.rename(columns = {'count': prefix_str + '_line_count', 'any_data': prefix_str + '_any_data'  }, inplace = True)

    return(obs_counts_df)
  
def get_time_per_day(aggregated_data, diff_seconds, prefix_str):
    """
    Reads in an aggregated dataframe, splits into increments where no gap is larger than num_seconds, and returns the sum of observed time
    
    Args:
        aggregated_data (DataFrame): a user's aggregated study data
        diff_seconds (int): the minimum difference in seconds between two groups
        prefix_str (str): the prefix to append to column names (like 'accelerometer' or 'gps')
    returns:
        obs_time_df (DataFrame): a dataframe with a column of dates and a column of observation time per date
        
    """
    if aggregated_data.shape[0] == 0:
        blank_df = pd.DataFrame(columns = ['date', prefix_str + '_sum_hours', prefix_str + '_any_data'])
        blank_df['date'] = blank_df['date'].astype('datetime64[ns]')
        return(blank_df)
    #sort just to make sure...
    aggregated_data.sort_values('local_time', inplace = True, ascending = True)
    aggregated_data['prev_local_time'] = aggregated_data['local_time'].shift(1) #move measurements forward
    #mark when grouping starts
    aggregated_data['new_measurement'] = np.where((aggregated_data['local_time'] 
                                                   - aggregated_data['prev_local_time'] ).dt.seconds > diff_seconds, 1, 0)
    #smallest and largest day found in this df
    min_day = np.min(aggregated_data['date'])
    max_day = np.max(aggregated_data['date'])
    date = min_day
    #blank df for adding to later
    obs_time_df_list = []
    while date <= max_day:
        #grab all lines in that day
        day_df = aggregated_data.loc[aggregated_data['UTC time'].dt.date == date, : ].copy()
        if day_df.shape[0] > 1: #must have at least 2 measurements to have a span
            #cumulative sum to group all of the same groupings together
            day_df['measurement_id'] = day_df['new_measurement'].cumsum()
            #first and last time in grouping
            day_df['first_time'] = day_df.groupby(['measurement_id'])['UTC time'].transform('min')
            day_df['last_time'] = day_df.groupby(['measurement_id'])['UTC time'].transform('max')
        
            day_df['diff_hours'] = (day_df['last_time'] - day_df['first_time']) / np.timedelta64(1, "h")
            unique_diff_hours = day_df.drop_duplicates(subset = ['measurement_id'])['diff_hours']
        
            temp_df = pd.DataFrame({'date': [date], 'sum_hours': [np.sum(unique_diff_hours)], 'any_data': [1]})
        
        else:
            if day_df.shape == 1:
                temp_df = pd.DataFrame({'date': [date], 'sum_hours': [0], 'any_data': [1]})
            else:
                temp_df = pd.DataFrame({'date': [date], 'sum_hours': [0], 'any_data': [0]})
        obs_time_df_list.append(temp_df)
        
        date = date + pd.Timedelta(1,unit = 'day') #increment to next day
    obs_time_df = pd.concat(obs_time_df_list, axis = 0, ignore_index = True) #bind dfs together
    obs_time_df.rename(columns = {'sum_hours': prefix_str + '_sum_hours', 'any_data': prefix_str + '_any_data'  }, inplace = True)

    return(obs_time_df)
