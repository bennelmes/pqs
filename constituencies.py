from importlib.resources import path
from tqdm import tqdm
import requests
import json
import pandas as pd
import glob
import os
from pathlib import Path
import geojson

def download_constituencies(path_to_tmp):
    """
    This method sweeps and downloads the latest data on constituencies. 
    :return: Two DataFrames, active_c_df, former_c_df
    """
    ########################
    # START API SWEEP      #
    ########################

    # First we determine which id numbers are real and which do not refer
    possible_numbers = [x for x in range(0, 5001)]

    # We iterate through the possible numbers and append those with response code 200 (success) to the actual numbers
    print('Getting new list of Constituency id numbers + info. This can take about 15 minutes...')

    for constituency_id in tqdm(possible_numbers):
        url = 'https://members-api.parliament.uk/api/Location/Constituency/{p}'.format(p=constituency_id)
        headers = {'accept': 'text/plain'}
        response = requests.get(url, headers=headers)
        # If the response is valid (i.e. there's an MP) save the info to the 
        if response.status_code == 200:
            data = response.json()
            if data['value']['endDate'] is None:
                with open(path_to_tmp+'/{}_active_info.json'.format(constituency_id), 'w') as outfile:
                    json.dump(data['value'], outfile)
            else:
                with open(path_to_tmp+'/{}_former_info.json'.format(constituency_id), 'w') as outfile:
                    json.dump(data['value'], outfile)
        else:
            pass

    # We'll combine the current constituencies into one big bit of json
    combined_json = []
    combined_former_json = []
    # Here we iterate through the downloaded json and add it into the big bit of json
    for f in tqdm(glob.glob(path_to_tmp+'/*_active_info.json')):
        with open(f, 'rb') as infile:
            combined_json.append(json.load(infile))
    # Iterating through more globs of json
    for f in tqdm(glob.glob(path_to_tmp+'/*_former_info.json')):
        with open(f, 'rb') as infile:
            combined_former_json.append(json.load(infile))

    active_constituencies_df = pd.json_normalize(combined_json)
    former_constituencies_df = pd.json_normalize(combined_former_json)

    ########################
    # START DATA WRANGLING #
    ########################

    # Cleaning up column names
    active_constituencies_df.columns = [x.replace('.', '') for x in active_constituencies_df.columns]
    former_constituencies_df.columns = [x.replace('.', '') for x in former_constituencies_df.columns]

    active_constituencies_df[active_constituencies_df.name.notna()]
    former_constituencies_df[former_constituencies_df.name.notna()]

    active_constituencies_df.columns = [x.replace('.', '_') for x in active_constituencies_df.columns]
    former_constituencies_df.columns = [x.replace('.', '_') for x in former_constituencies_df.columns]
    

    ######################
    # END DATA WRANGLING #
    ######################

    #######################
    # START FILE CLEAN-UP #
    #######################

    # Get a list of all the file paths that ends with .txt from in specified directory
    fileList = glob.glob(path_to_tmp+'/*_active_info.json')
    # Iterate over the list of filepaths & remove each file.
    print('Cleaning up... active_info.json files...')
    for filePath in tqdm(fileList):
        try:
            os.remove(filePath)
        except:
            print("Error while deleting file : ", filePath)

    # Get a list of all the file paths that ends with .txt from in specified directory
    fileList = glob.glob(path_to_tmp+'/*_former_info.json')
    # Iterate over the list of filepaths & remove each file.
    print('Cleaning up... former_info.json files...')
    for filePath in tqdm(fileList):
        try:
            os.remove(filePath)
        except:
            print("Error while deleting file : ", filePath)
    print('All done updating the Parliamentarians with latest data! Be on your merry way.')

    #######################
    # END FILE CLEAN-UP   #
    #######################
    active_constituencies_df.to_csv(path_to_tmp+'/active_constituencies.csv', index=False)
    former_constituencies_df.to_csv(path_to_tmp+'/former_constituencies.csv', index=False)
    return active_constituencies_df, former_constituencies_df

def download_shapefiles(path_to_tmp):
    print('Downloading Parliamentary constituency shapefiles, December 2020 versions. \nUpdate link in constituencies.download_shapefiles to get more recent boundaries. ')
    r = requests.get('https://opendata.arcgis.com/datasets/19841da5f8f6403e9fdcfb35c16e11e9_0.geojson')
    gj = geojson.loads(r.content)
    with open(path_to_tmp+'/pcon_boundaries.geojson', 'w') as f:
        geojson.dump(gj, f)

if __name__ == '__main__':
    
    Path('tmp').mkdir(parents=True, exist_ok=True)

    if not os.path.exists('tmp/active_constituencies.csv'):
        download_constituencies(path_to_tmp='tmp')
    else:
        pass

    if not os.path.exists('tmp/pcon_boundaries.geojson'):
        download_shapefiles(path_to_tmp='tmp')
    
    