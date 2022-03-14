import requests
import json
import pandas as pd
import glob
from tqdm import tqdm
import os


class UKParliament:
    """
    A class to retrieve information about MPs and constituencies from Parliament's API.

    The class works by downloading the latest data about current and former members of the Houses of Parliament using the "download_mps" method. This method creates a series of temporary files and objects, which the other methods make use of later. 

    """

    def __init__(self, path_to_tmp):
        self.path_to_tmp = path_to_tmp

    def download_mps(self):
        """
        This method performs an initial sweep of the database to obtain current and former MP and Peer info. 
        In the tmp folder, it saves four files:
        * active_members.csv - a csv of all active members of the House of Commons
        * former_members.csv - a csv of all former members of the House of Commons
        * active_commons.csv - a json file of all active members of the House of Commons
        * active_lords.csv - a json file of all former members of the House of Commons

        :return: Two DataFrames: active_members_df, former_members_df
        """
        ########################
        # START API SWEEP      #
        ########################

        # This section of code polls Parliament's Members API with every possible MP id. It has been determined that MPs have an id no higher than 5000.

        # First we determine which id numbers are real and which do not refer
        possible_numbers = [x for x in range(1, 5001)]
        actual_numbers = []

        # We iterate through the possible numbers and append those with response code 200 (success) to the actual numbers
        print('Getting new list of MP id numbers + info. This can take about 15 minutes...')

        active_members = []
        former_members = []

        # Go through each possible MP id and download the response, if successful, and save as a json file. 
        for mp in tqdm(possible_numbers):
            url = 'https://members-api.parliament.uk/api/Members/{mp}'.format(mp=mp)
            headers = {'accept': 'text/plain'}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data['value']['latestHouseMembership']['membershipEndDate'] is None:
                    with open(self.path_to_tmp+'/{}_active_info.json'.format(mp), 'w') as outfile:
                        json.dump(data['value'], outfile)
                    active_members.append(mp)
                else:
                    with open(self.path_to_tmp+'/{}_former_info.json'.format(mp), 'w') as outfile:
                        json.dump(data['value'], outfile)
                    former_members.append(mp)
            else:
                pass
        
        # We iterate through the downloaded json and add it into the big bit of json

        combined_json = []
        combined_former_json = []

        for f in tqdm(glob.glob(self.path_to_tmp+'/*_active_info.json')):
            with open(f, 'rb') as infile:
                combined_json.append(json.load(infile))

        for f in tqdm(glob.glob(self.path_to_tmp+'/*_former_info.json')):
            with open(f, 'rb') as infile:
                combined_former_json.append(json.load(infile))

        ######################
        # IMPORT INTO PANDAS #
        ######################

        active_members_df = pd.json_normalize(combined_json)
        former_members_df = pd.json_normalize(combined_former_json)

        ##################
        # DATA WRANGLING #
        ##################
        # Cleaning up column names
        active_members_df.columns = [x.replace('.', '') for x in active_members_df.columns]
        former_members_df.columns = [x.replace('.', '') for x in former_members_df.columns]

        # Cleaning up House Membership column
        active_members_df['latestHouseMembershiphouse'] = active_members_df['latestHouseMembershiphouse'].apply(lambda x: 'Commons' if x==1 else 'Lords')
        former_members_df['latestHouseMembershiphouse'] = former_members_df['latestHouseMembershiphouse'].apply(lambda x: 'Commons' if x==1 else 'Lords')

        # Create a surname column
        active_members_df['surname'] = active_members_df.nameListAs.apply(lambda x: x.split(', ')[0])
        active_members_df['firstname'] = active_members_df.nameListAs.apply(lambda x: x.split(', ')[1])
        former_members_df['surname'] = active_members_df.nameListAs.apply(lambda x: x.split(', ')[0])
        active_members_df['firstname'] = active_members_df.apply(lambda row: '' if row['latestHouseMembershiphouse'] == 'Lords' else row['firstname'], axis=1)
        active_members_df['firstname'] = active_members_df.firstname.apply(lambda x: x.replace('Mr ', '').replace('Mrs ', '').replace('Ms ', '').replace('Sir ', '').replace('Dr ', '').replace('Miss ', '').replace('Dame ', ''))
        active_members_df['firstname'] = active_members_df.firstname.apply(lambda x: x.split(' ')[0])

        ##################
        # CLEAN UP FILES #
        ##################

        # Get a list of all the file paths that ends with .txt from in specified directory
        fileList = glob.glob(self.path_to_tmp+'/*_active_info.json')
        # Iterate over the list of filepaths & remove each file.
        print('Cleaning up... active_info.json files...')
        for filePath in tqdm(fileList):
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)

        # Get a list of all the file paths that ends with .txt from in specified directory
        fileList = glob.glob(self.path_to_tmp+'/*_former_info.json')

        # Iterate over the list of filepaths & remove each file.
        print('Cleaning up... former_info.json files...')
        for filePath in tqdm(fileList):
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)
        print('All done updating the Parliamentarians with latest data! Be on your merry way.')

        ##################
        # SAVE FILES     #
        ##################
        self.active_members_df = active_members_df
        self.former_members_df = former_members_df
        self.state_of_parties = active_members_df.groupby(['latestPartyid', 'latestPartyname']).count().iloc[:, :1].reset_index().rename(columns={'id': 'number'})
        self.active_commons_df = active_members_df[active_members_df.latestHouseMembershiphouse == 'Commons']
        self.active_lords_df = active_members_df[active_members_df.latestHouseMembershiphouse == 'Lords']
        active_members_df.to_csv(self.path_to_tmp+'/active_members.csv', index=False)
        former_members_df.to_csv(self.path_to_tmp+'/former_members.csv', index=False)
        self.active_commons_df.to_csv(self.path_to_tmp+'/active_commons.csv', index=False)
        self.active_lords_df.to_csv(self.path_to_tmp+'/active_lords.csv', index=False)

        return active_members_df, former_members_df
    
    def download_constituencies(self):
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
                    with open(self.path_to_tmp+'/{}_active_info.json'.format(constituency_id), 'w') as outfile:
                        json.dump(data['value'], outfile)
                else:
                    with open(self.path_to_tmp+'/{}_former_info.json'.format(constituency_id), 'w') as outfile:
                        json.dump(data['value'], outfile)
            else:
                pass

        # We'll combine the current constituencies into one big bit of json
        combined_json = []
        combined_former_json = []
        # Here we iterate through the downloaded json and add it into the big bit of json
        for f in tqdm(glob.glob(self.path_to_tmp+'/*_active_info.json')):
            with open(f, 'rb') as infile:
                combined_json.append(json.load(infile))
        # Iterating through more globs of json
        for f in tqdm(glob.glob(self.path_to_tmp+'/*_former_info.json')):
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
        fileList = glob.glob(self.path_to_tmp+'/*_active_info.json')
        # Iterate over the list of filepaths & remove each file.
        print('Cleaning up... active_info.json files...')
        for filePath in tqdm(fileList):
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)

        # Get a list of all the file paths that ends with .txt from in specified directory
        fileList = glob.glob(self.path_to_tmp+'/*_former_info.json')
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
        active_constituencies_df.to_csv(self.path_to_tmp+'/active_constituencies.csv', index=False)
        former_constituencies_df.to_csv(self.path_to_tmp+'/former_constituencies.csv', index=False)
        return active_constituencies_df, former_constituencies_df
    
    def get_job_history(self, api_number, create_id_col = False):
        """
        This method is designed to be used iteratively, on a list of API numbers to generate MP job history info. It returns a DataFrame with job history information.
        If the request is not successful, then nothing is returned. 
        :param api_number: Int, the unique identifier in Parliament's database for the MP or Lord in question. 
        :param create_id_col: bool, default False. If you want to create a separate id column with autoincremented integers starting at id=1, then set to True. 
        :return: a DataFrame if the request to Parliament's API is successful, else no return is made. 
        """
        headers = {
            'accept': 'text/plain',
        }
        response = requests.get('https://members-api.parliament.uk/api/Members/{n}/Biography'.format(n=api_number), headers=headers)
        if response.status_code == 200:
            data = response.json()

            # From the response, we obtain the nested data, which is sorted into three kinds of jobs
            if len(data['value']['governmentPosts']) > 0:
                govt_jobs = pd.DataFrame(data['value']['governmentPosts'])
            else:
                govt_jobs = pd.DataFrame(columns=['house', 'name', 'id', 'startDate', 'endDate', 'additionalInfo', 'additionalInfoLink'])

            if len(data['value']['oppositionPosts']) > 0:
                oppo_jobs = pd.DataFrame(data['value']['oppositionPosts'])
            else:
                oppo_jobs = pd.DataFrame(columns=['house', 'name', 'id', 'startDate', 'endDate', 'additionalInfo', 'additionalInfoLink'])
            
            if len(data['value']['committeeMemberships']) > 0:
                cttee_jobs = pd.DataFrame(data['value']['committeeMemberships'])
            else:
                cttee_jobs = pd.DataFrame(columns=['house', 'name', 'id', 'startDate', 'endDate', 'additionalInfo', 'additionalInfoLink'])
            # The committee jobs dataframe needs some tidying to distinguish between membership and chairmanship of the committee. 
            # BELOW WAS TIDIED UP DUE TO AN ERROR WHEN THERE WERE NO COMMITTEE MEMBERSHIPS
            # Add 'Member of' in front of committee jobs
            # print(govt_jobs.columns.tolist())
            # cttee_jobs['name'] = cttee_jobs['name'].apply(lambda x: 'Member of ' + x if 'Committee' in x else x)
            # print(cttee_jobs.columns.tolist())

            # Replace 'Member of' with 'Chair of' if additionalInfo column indicates that they were a chair of the committee
            # cttee_jobs['name'] = cttee_jobs.apply(lambda row: row['name'].replace('Member of ', 'Chair of ') if (row['additionalInfo'] == 'Chaired') else row['name'], axis=1)

            # Now we concatenate the three dataframes into one big one. They all have the same columns, so this is easy. 
            jobs_df = pd.concat([govt_jobs, oppo_jobs, cttee_jobs])
            if jobs_df.shape[0] > 0:
                # Add 'Member of' in front of committee jobs
                jobs_df['name'] = jobs_df['name'].apply(lambda x: 'Member of ' + x if 'Committee' in x else x)
                # Replace 'Member of' with 'Chair of' if additionalInfo column indicates that they were a chair of the committee
                jobs_df['name'] = jobs_df.apply(lambda row: row['name'].replace('Member of ', 'Chair of ') if (row['additionalInfo'] == 'Chaired') else row['name'], axis=1)

                # Make a column that specifies the MPs' Parliament API number so we have some reference back to the MP
                jobs_df['mp_id'] = api_number
                # Now we merge them with self.mps, which has API number + contact_id number from CiviCRM
                jobs_df = jobs_df.merge(self.mps, how='inner', left_on='mp_id', right_on='parliament_api_number_68')
                # Drop a few unecessary columns - we need to start making this DataFrame look like our target SQL table.
                jobs_df.drop(columns = ['id_x', 'id_y', 'parliament_api_number_68', 'house', 'mp_id', 'additionalInfoLink'], inplace=True, errors='ignore')
                # In this try and except clause, we try to obtain the necessary columns for our target table. Of course, it's possible that MPs  have no job info, if they've never had a parliamentary job. 
                # In these cases, there will be an error, and we simple pass and return no data, since there's no job info to be found. That's why the except passes on a 'keyerror'. 
                # 
                try:
                    jobs_df = jobs_df[['entity_id', 'name', 'startDate', 'endDate', 'additionalInfo']]
                    jobs_df.columns = ['entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']
                    jobs_df.drop_duplicates(inplace=True)
                    # This provisions some functionality around an id column. Not useful when doing mass scrapes. 
                    if create_id_col:
                        jobs_df['id'] = jobs_df.index + 1
                        jobs_df = jobs_df[['id', 'entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']]
                    else:
                        pass
                    return jobs_df
                except KeyError:
                    pass
            else:
                jobs_df = pd.DataFrame(columns=['house', 'name', 'id', 'startDate', 'endDate', 'additionalInfo', 'additionalInfoLink'])
                return jobs_df
        else:
            pass

    def get_party(self):
        list_of_api_nos = self.mps_ids
        list_of_dfs = []
        """
        A function to get the party of a particular mp. 
        :return: a DataFrame with columns...
        """
        for mp in tqdm(list_of_api_nos):
            url = 'https://members-api.parliament.uk/api/Members/{mp}'.format(mp=mp)
            headers = {
            'accept': 'text/plain',
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                data['value']['latestParty']['mpId'] = mp
                df = pd.DataFrame(data['value']['latestParty'], index=[0])[['id', 'name', 'abbreviation', 'mpId']]
                list_of_dfs.append(df)
            else:
                pass
        df = pd.concat(list_of_dfs)
        return df


    def update_mp_job_info(self, path_to_tmp_folder):
        """
        This function returns a DataFrame containing all MP job information.
        """

        # mps_list = self.mps.parliament_api_number_10.tolist()
        print('Got preliminary info, now updating job history. This will take a while. ')
        for mp in tqdm(self.mps_ids):
            # try:
            df = self.get_job_history(mp)
            df.to_csv(path_to_tmp_folder+'/mp_{}.csv'.format(mp))
            # except:
            #     print('excepted')
            #     pass
            
        df = pd.concat([pd.read_csv(file, index_col=0) for file in glob.glob(path_to_tmp_folder+'/mp_*.csv')], ignore_index=True)
        df.drop_duplicates(inplace=True)
        df['id'] = df.index + 1
        df = df[['id', 'entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']]
        df['is_current_job_67'] = None
        df['frontbench_job_69'] = None
        return df


    def get_details(self, site_key, user_key):
        """
        This function takes a list of Parliamentary API numbers and checks whether there is a contact present in the CiviCRM database. 

        It is intended to be used on the list of 
        :return: A dataframe of the latest active parliamentarians (taken from the last download from UKParliament.download_mps(), and a json object with the details of Parliamentarians who are present in the Civi database.)
        """

        # Import data from tmp folder
        active_commons = pd.read_csv(self.path_to_tmp+'/active_commons.csv')

        # Join Commons and Lords into a master list of active Parliamentarians
        active_p = active_commons.copy() #pd.concat([active_commons, active_lords])
        mp_id_lst = active_p.id.tolist()

        # Blank list which will be populated and then returned. 
        active_members_in_civi = []

        # Our CRM endpoint
        url = 'https://civi.newautomotive.org/wp-content/plugins/civicrm/civicrm/extern/rest.php'

        # Iterate through the list of Parliamentary API numbers. 
        for id in tqdm(mp_id_lst):
                params = {
                'entity': 'Contact',
                'action': 'get',
                'json': 1, 
                "debug":1,
                "sequential":1,
                "return":"custom_68,sort_name,first_name,last_name",
                # "custom_64":"Active",
                # "custom_65":"Lords",
                "custom_68" : id,
                'api_key' : user_key,
                'key': site_key
                }
                r = requests.get(url=url, params=params)
                if r.json()['count'] == 1:
                    data = r.json()['values'][0]
                    active_members_in_civi.append(data)
                elif r.json()['count'] > 1:
                    print('Problem with id #{mpid}'.format(mpid=id))
                else:
                    pass
                
        not_upload_ids = [int(x['custom_68']) for x in active_members_in_civi]
        active_members_not_in_civi = active_p[~active_p.id.isin(not_upload_ids)]

        return active_p, active_members_not_in_civi

    def create_parliamentarian(site_key, user_key, mp_id, sort_name, party, last_name, display_name, legal_name, house, first_name):
        """
        Create a new parliamentarian in our CiviCRM database. NB this will not work unless you configure it for your own CiviCRM install. The custom field names will be different. 
        :return: Nothing. 
        """
        mp_id = str(mp_id)

        url = 'https://civi.newautomotive.org/wp-content/plugins/civicrm/civicrm/extern/rest.php'

        params = {
        'entity': 'Contact',
        'action': 'create',
        'json': 1, 
        "debug":1,
        "sequential":1,
        "contact_type":"Individual",
        "contact_sub_type":"Member_of_UK_Parliament",
        "sort_name":sort_name,
        "custom_68":mp_id,
        "custom_70":party,
        "custom_65":house,
        "custom_64":"Active",
        "last_name":last_name,
        "first_name":first_name,
        # "custom_64":"Active",
        # "custom_65":"Lords",
        'api_key' : user_key,
        'key': site_key,
        "display_name":display_name,
        "legal_name":legal_name
        }
        requests.post(url=url, params=params)