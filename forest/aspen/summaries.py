import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import argparse
import mano
import requests
import mano.sync as msync
import mano
import shutil

from .functions import get_time_per_day, get_count_per_day, read_and_aggregate, count_files
ALL_DATA_STREAMS = ['accelerometer', 'ambient_audio', 'app_log', 'audio_recordings', 'bluetooth',
'calls', 'devicemotion', 'gps', 'gyro', 'identifiers', 'image_survey', 'ios_log', 'magnetometer', 'power_state', 'proximity', 'reachability',
'survey_answers', 'survey_timings', 'texts', 'wifi']

ALL_DATA_STREAMS = ['accelerometer', 'ambient_audio', 'app_log', 'audio_recordings', 'bluetooth',
'calls', 'devicemotion', 'gps', 'gyro', 'identifiers', 'image_survey', 'ios_log', 'magnetometer', 'power_state', 'proximity', 'reachability',
'survey_answers', 'survey_timings', 'texts', 'wifi']

LINE_COUNT_STREAMS = ['app_log','calls','image_survey','ios_log', 'power_state', 'proximity', 'reachability', 'survey_answers', 'survey_timings', 'texts']
FILE_COUNT_STREAMS = ['audio_recordings', 'ambient_audio']
TIME_COUNT_DICT = {'accelerometer': 30, 'bluetooth': 660, 'devicemotion':660, 'gps':900, 'gyro':30, 'magnetometer':660}




def download_data(keyring,  study_id, download_folder, users = [], time_start = "2008-01-01", 
                      time_end = None, data_streams = None):
    '''
    Downloads all data
    
    Args: 
        keyring: a keyring generated by mano.keyring
    
        users(iterable): A list of users to download data for. If none are entered, it attempts to download data for all users
        
        study_id(str): The id of a study
        
        download_folder(str): path to a folder to download data
        
        time_start(str): The initial date to download data (Formatted as in '2008-01-01')
        
        time_end(str): The date to end downloads. The default is today at midnight.
        
        data_streams(iterable): A list of all data streams to download. The default (None) is all possible data streams. 
        
    '''
    if not os.path.isdir(download_folder):
        os.mkdir(download_folder)
    
    if time_end is None:
        time_end = datetime.today().strftime("%Y-%m-%d")+"T23:59:00"
        
    if users == []:
        print('Obtaining list of users...')
        users = [u for u in mano.users(keyring, study_id)]
    
    for u in users:
        zf = None
        try:
            print(f'Downloading data for {u}')
            zf = msync.download(keyring, study_id, u, data_streams, time_start = time_start, time_end = time_end)
            if zf is not None:
                zf.extractall(download_folder)
        except requests.exceptions.ChunkedEncodingError:
            print(f'Network failed in download of {u}')
            pass

        if zf is None:
            print(f'No data for {u}; nothing written')







def monitor_data_quantities(download_folder, output_folder, 
line_count_streams = LINE_COUNT_STREAMS, file_count_streams = FILE_COUNT_STREAMS, 
time_count_dict = TIME_COUNT_DICT, tz_str = "UTC"):
    '''
    Generates a csv file with data quantities downloaded by date
    
    Args:
        download_folder(str): filepath to the folder where the study's data is
        
        output_folder(str): filepath in which to put summary spreadsheets
        
        line_count_streams(iterable): List of data streams to compute line count statistics on
        
        file_count_streams(iterable): List of data streams to compute file count statistics on
        
        time_count_dict(dict): Dictionary with a key for every time count string and a value for the number of seconds for the minimum missing "span" used

        tz_str(str): Time zone (formatted as seen in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
    
    Returns:
      None (writes a csv file)
        
    '''
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    
    all_users_df_list = []
    
    users = [x for x in os.listdir(download_folder) if (os.path.isdir(os.path.join(download_folder,x)))]

    
    for u in users:
        user_df = pd.DataFrame(columns = ['date']) #We will merge all dfs with this one
        user_df['date'] = user_df['date'].astype('datetime64[ns]')

        for d_stream in file_count_streams:
            summary_df = count_files(download_folder, u, d_stream, tz_str)
            user_df = user_df.merge(summary_df, on = 'date', how = 'outer')
        for d_stream in line_count_streams:
            agg_df = read_and_aggregate(download_folder = download_folder, beiwe_id = u, data_stream = d_stream, tz_str = tz_str)

            summary_df = get_count_per_day(aggregated_data = agg_df, prefix_str = d_stream)
            user_df = user_df.merge(summary_df, on = 'date', how = 'outer')
        for d_stream in time_count_dict.keys():
            agg_df = read_and_aggregate(download_folder = download_folder, beiwe_id = u, data_stream = d_stream, tz_str = tz_str)
            summary_df = get_time_per_day(aggregated_data = agg_df, diff_seconds = time_count_dict[d_stream], prefix_str = d_stream)
            user_df = user_df.merge(summary_df, on = 'date', how = 'outer')

        user_df['beiwe_id'] = u
        user_df.set_index(['beiwe_id', 'date'], drop = True, inplace = True) #print these first
        user_df.fillna(0, inplace = True)
        user_df.to_csv(os.path.join(output_folder, u + ".csv"), index = True)
        all_users_df_list.append(user_df)
    if len(all_users_df_list) > 0:
        output_df = pd.concat(all_users_df_list, axis = 0, sort=False)
        output_df.to_csv(os.path.join(output_folder, 'agreggated_summaries.csv'), index = True)


def by_survey_administration_from_agg(sycamore_folder):
    '''
    Takes an agg_survey_data csv file and writes individual csv files for each survey
    
    Args:
        sycamore_folder(str): filepath to the output generated by sycamore. 
        
    Returns:
        None. Writes files with information. 
    
    '''
    in_path = os.path.join(sycamore_folder, 'agg_survey_data.csv')
    df = pd.read_csv(in_path)
    df['Local time'] = pd.to_datetime(df['Local time'])
    
    surveys_list = []

    for survey_id in df['survey id'].unique():
        print(f'now processing: {survey_id}')
        survey_df = df.loc[df['survey id'] == survey_id].copy()
        unique_survey_cols = ['beiwe_id', 'surv_inst_flg']
        
        #get starting and ending times for each survey
        survey_df['start_time'] = survey_df.groupby(unique_survey_cols)['Local time'].transform('first')
        survey_df['end_time'] = survey_df.groupby(unique_survey_cols)['Local time'].transform('last')
        
        survey_df = survey_df.loc[survey_df['submit_line'] != 1]  
        #needs to come after finding start and end times because
        #the "user hit submit" line contains end times
        
        if survey_df.shape[0] >0: #We need to have data to read
            survey_df.sort_values(by = ['beiwe_id', 'Local time'], ascending = True, inplace = True)
            survey_df.reset_index(inplace = True)
            
        # I could probably replace this with reading the study format file, but that would make things 
        # less simple for the user
            id_text_dict = {}.fromkeys(survey_df['question id'].unique())
    
            num_found = 0
            i = 0
            keys_not_found = True
            while keys_not_found:
                
                #update dictionary entry
                if id_text_dict[survey_df.loc[i, 'question id']] == None:
                    id_text_dict[survey_df.loc[i, 'question id']] = [survey_df.loc[i, 'question text'], 
                        survey_df.loc[i, 'question type'], survey_df.loc[i, 'question answer options']]
                    num_found = num_found + 1
                if num_found == len(id_text_dict):
                    keys_not_found = False
                i = i + 1
                if i == survey_df.shape[0]: #should find all keys before we get to the end but let's be safe...
                    break
    
            
            survey_df['survey_duration'] = survey_df['end_time'] - survey_df['start_time'] 
            
            
            keep_cols = ['beiwe_id', 'start_time', 'end_time', 'survey_duration']
            
            unique_question_cols = keep_cols + ['question id']
            survey_df.drop_duplicates(unique_question_cols, keep = 'last', inplace = True) #Because we sorted ascending, this will keep the most recent response
            pivot_df = survey_df.pivot(index = keep_cols, \
                              columns = 'question id', values = 'answer')
            question_info_df = pd.DataFrame(id_text_dict)
            
            question_id_df = pd.DataFrame(columns = question_info_df.columns)
            question_id_df.loc[0] = question_info_df.columns #move column names to a row for writing
            ## add fake indices to stack nicely with the multiindex
            for col in keep_cols[1:4]:
                question_info_df[col] = ''
                question_id_df[col] = ''

            question_id_df['beiwe_id'] = 'Question ID'
            question_info_df['beiwe_id'] = ['Question Text','Question Type','Question Options']
            ## Get these to stack nicely with multiindex
            question_info_df.set_index(keys = keep_cols, inplace = True)
            question_id_df.set_index(keys = keep_cols, inplace = True)
            ## stack together
            output_df = pd.concat([question_info_df,question_id_df, pivot_df])
            output_df = output_df.reset_index(drop = False)
            ## Interpretable column names in csv
            colnames = ['beiwe_id', 'start_time','end_time','survey_duration']
            colnames = colnames +  [f"question_{i+1}" for i in range(len(output_df.columns) - len(colnames))]
            output_df.columns = colnames
            
            output_df.to_csv(os.path.join(sycamore_folder, survey_id + ".csv"), index = False)
    
    
    

