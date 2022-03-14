import requests
import pandas as pd
from tqdm import tqdm
from UKParliament import UKParliament

def main():
    tmp = '/Users/ben/Documents/blog/pqs/tmp'
    # Instantiate our UKParliament downloader, and get the latest MPs
    ukp = UKParliament(path_to_tmp= tmp)
    ukp.download_mps()

    # Query the civiCRM API to check for any active MPs who are not in the database at the moment
    # active_mps, mps_to_upload = ukp.get_details(site_key = site_key, user_key=user_key)

if __name__ == '__main__':
    main()
