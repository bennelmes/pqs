import numpy as np
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

# A function that will download WPQs for a given range of dates. (NB this will crash if the date range is too wide)
def get_wpqs_by_answered(answeredWhenFrom, answeredWhenTo, answered=None):
    """
    Download WPQs answered between a given range (recommended not to specify longer than 1 month). Dates should be in the format 'yyyy-mm-dd'.
    :param: tabledWhenFrom str 'yyyy-mm-dd'
    :param: tabledWhenTo str 'yyyy-mm-dd'
    :param: answered bool default None. If True, the function only downloads answered PQs. If False, only unanswered. If None, all are downloaded.  
    :return: a list of WQPs expressed in dictionaries. 
    """
    url = 'https://writtenquestions-api.parliament.uk/api/writtenquestions/questions'
    if answered:
        params = {
            # 'askingMemberId':  4663,
            'answeredWhenFrom': answeredWhenFrom,
            'answeredWhenTo': answeredWhenTo,
            'take':1000000,
            'answered': 'Answered'
            }
    elif not answered:
        params = {
            # 'askingMemberId':  4663,
            'answeredWhenFrom': answeredWhenFrom,
            'answeredWhenTo': answeredWhenTo,
            'take':1000000,
            'answered': 'Unanswered'
            }
    elif answered is None:
        params = {
            # 'askingMemberId':  4663,
            'answeredWhenFrom': answeredWhenFrom,
            'answeredWhenTo': answeredWhenTo,
            'take':1000000,
            # 'answered': 'Unanswered'
            }

    headers = {'Accept': 'application/json'}


    r = requests.get(url=url, params = params, headers=headers)
    data = [x['value'] for x in r.json()['results']]
    return data


def update_answered_pqs(tmp = '/Users/ben/Documents/blog/UKParliament/tmp'):
    """
    A function that downloads an archive of all answered WPQs. It looks for an archive, and then downloads WQPs using date as an input, making monthly calls to Parliament's API starting with the earliest date for which data is available. 

    If there is an archive, it checks when it was last updated, and looks for WPQs which have been answered since the last update, and appends them to the database. 
    :return: a pandas Dataframe of all answered WPQs. 
    """
    # Declare some datetime variables
    # We use these to generate the complete archive since 2014-05, when the digital record begins. 
    today = datetime.today()
    today_str = today.strftime("%Y-%m-%d")
    tomorrow = today + relativedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    start_date = '2014-05-01'
    next_month = today + relativedelta(months=1)
    next_month_str = next_month.strftime("%Y-%m-%d")

    # Check if there's already a file downloaded. If yes, download questions tabled since last update. If not, download alles.
    if Path(tmp+'/pqs.csv', index_col=0).is_file():
        pqs = pd.read_csv(Path(tmp+'/pqs.csv'))
        pqs['dateAnswered'] = pd.to_datetime(pqs.dateAnswered)
        pqs = pqs.drop_duplicates()
        no_pqs = pqs.shape[0]
        print(pqs.shape)
        start_date = pqs.dateAnswered.max()#.strftime("%Y-%m-%d")
        # start_date = pd.to_datetime('2022-01-15')
        print("File pqs.csv found with {n} WPQs in, last updated {d}. Proceeding to update.".format(n=no_pqs, d=start_date.strftime("%Y-%m-%d")))

        date_list = pd.date_range(start_date, today, freq='D').strftime('%Y-%m-%d').tolist()
        # print(date_list)
        date_list.append(tomorrow_str)

        master_wpqs = []

        for i, e in enumerate(tqdm(date_list, leave=False)):
            try:
                lst_wpqs = get_wpqs_by_answered(answeredWhenFrom=date_list[i], answeredWhenTo=date_list[i+1], answered=True)
                for x in lst_wpqs:
                    master_wpqs.append(x)
            except IndexError:
                pass

        n_pqs = pd.DataFrame(master_wpqs)
        n_pqs.drop(columns=['attachments', 'groupedQuestions', 'groupedQuestionsDates'], inplace=True)
        n_pqs['dateAnswered'] = pd.to_datetime(n_pqs.dateAnswered)
        n_pqs = n_pqs.drop_duplicates()

        old_length = pqs.shape[0]
        print(old_length)
        print(n_pqs.shape)


        new_pqs = pd.concat([pqs, n_pqs])
        new_pqs['dateAnswered'] = pd.to_datetime(new_pqs.dateAnswered)
        new_pqs = new_pqs.drop_duplicates()
        new_length = new_pqs.shape[0]
        print(new_length)

        pqs_added = new_length - old_length
        print("Downloaded {n} WPQs, which have been add to the archive.".format(n=pqs_added))
        new_pqs.to_csv(Path(tmp+'/pqs.csv'), index=False, index_label=False)
        test = pd.read_csv(Path(tmp+'/pqs.csv'))
        print(test.shape)
        print("Test shape above")
        return new_pqs


    # Now handle situations where there's no file. 
    else:
        # If there's no file already downloaded, then we'll download everything since the beginning of time (May 2014, according to Parliament's API)
        print("No file found, proceeding with download of full archive. Sit tight, this can take about 20 minutes!")
        min_date_list = pd.date_range(start_date, today, freq='MS').strftime('%Y-%m-%d').tolist()
        max_date_list = pd.date_range(start_date, next_month, freq='M').strftime('%Y-%m-%d').tolist()
        # max_date_list.append(next_month_str)

        # List for all the PQS to go in:
        master_wpqs = []

        # Iterate through idx, get unanswered wpqs from each month and append to the master_wpqs list
        for i, e in enumerate(tqdm(max_date_list, leave=False)):
            lst_wpqs = get_wpqs_by_answered(answeredWhenFrom=min_date_list[i], answeredWhenTo=max_date_list[i], answered=True)
            for x in lst_wpqs:
                master_wpqs.append(x) 

        # Convert to DataFrame
        pqs = pd.DataFrame(master_wpqs)
        pqs.drop(columns=['attachments', 'groupedQuestions', 'groupedQuestionsDates'], inplace=True)
        # pqs.drop_duplicates(inplace=True)
        pqs.to_csv(Path(tmp+'/pqs.csv'), index=False, index_label=False)
        print('Full archive downloaded up to {d}. To get WPQs tabled since that date, call this function once more.'.format(d=pqs.dateTabled.max().strftime('%Y-%m-%d')))
        return pqs
    
    
    
"""
This section contains two functions, which when used together will download an archive of all tabled PQs, but with no information about their answer.
"""
# Create a database of all PQs, whether answered or not

# A useful function that will download WPQs for a given range of dates the question is tabled
def get_wpqs_by_date(tabledWhenFrom, tabledWhenTo):
    """
    Download WPQs tabled between a given range (recommended not to specify longer than 1 month). Dates should be in the format 'yyyy-mm-dd'.
    :param: tabledWhenFrom str 'yyyy-mm-dd'
    :param: tabledWhenTo str 'yyyy-mm-dd'
    :param: answered bool default None. If True, the function only downloads answered PQs. If False, only unanswered. If None, all are downloaded.  
    :return: a list of WQPs expressed in dictionaries. 
    """
    url = 'https://writtenquestions-api.parliament.uk/api/writtenquestions/questions'

    params = {
        'tabledWhenFrom': tabledWhenFrom,
        'tabledWhenTo': tabledWhenTo,
        'take':1000000,
        }

    headers = {'Accept': 'application/json'}

    r = requests.get(url=url, params = params, headers=headers)
    data = [x['value'] for x in r.json()['results']]
    return data


def download_ua_pqs(tmp = '/Users/ben/Documents/blog/pqs/tmp'):

    # Declare some datetime variables
    # We use these to generate the complete archive since 2014-05, when the digital record begins. 
    today = datetime.today()
    today_str = today.strftime("%Y-%m-%d")
    tomorrow = today + relativedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    start_date = '2014-05-01'
    next_month = today + relativedelta(months=1)
    next_month_str = next_month.strftime("%Y-%m-%d")


    if Path(tmp+'/ua_pqs.csv', index_col=0).is_file():
        pqs = pd.read_csv(Path(tmp+'/ua_pqs.csv'))
        no_pqs = pqs.shape[0]
        pqs['dateTabled'] = pd.to_datetime(pqs.dateTabled)
        start_date = pqs.dateTabled.max()#.strftime("%Y-%m-%d")
        # start_date = pd.to_datetime('2022-01-15')
        print("File ua_pqs.csv found with {n} WPQs in, last updated {d}. Proceeding to update.".format(n=no_pqs, d=start_date.strftime("%Y-%m-%d")))

        date_list = pd.date_range(start_date, today, freq='D').strftime('%Y-%m-%d').tolist()
        
        date_list.append(tomorrow_str)

        master_wpqs = []

        for i, e in enumerate(tqdm(date_list, leave=False)):
            try:
                lst_wpqs = get_wpqs_by_date(tabledWhenFrom=date_list[i], tabledWhenTo=date_list[i+1])
                for x in lst_wpqs:
                    master_wpqs.append(x)
            except IndexError:
                pass
        n_pqs = pd.DataFrame(master_wpqs)
        try:
            n_pqs.drop(columns=['attachments', 'groupedQuestions', 'groupedQuestionsDates'], inplace=True)
            n_pqs.drop(columns=[
                'isWithdrawn',
                'isNamedDay',
                'answerIsHolding',
                'answerIsCorrection',
                'answeringMemberId',
                'answeringMember',
                'correctingMemberId',
                'correctingMember',
                'dateAnswered',
                'answerText',
                'originalAnswerText',
                'comparableAnswerText',
                'dateAnswerCorrected',
                'dateHoldingAnswer',
                'attachmentCount',
            ], inplace=True)
        except KeyError:
            pass

        old_length = pqs.shape[0]
        new_pqs = pd.concat([pqs, n_pqs])
        
        new_pqs['dateTabled'] = pd.to_datetime(new_pqs.dateTabled)
        new_pqs = new_pqs.drop_duplicates()
        new_length = new_pqs.shape[0]
        pqs_added = new_length - old_length
        print("Downloaded {n} WPQs, which have been add to the archive.".format(n=pqs_added))
        new_pqs.to_csv(Path(tmp+'/ua_pqs.csv'), index=False, index_label=False)
        print("All done, be on your merry way.")
        return new_pqs


    # Now handle situations where there's no file. 
    else:
        # If there's no file already downloaded, then we'll download everything since the beginning of time (May 2014, according to Parliament's API)
        print("No file found, proceeding with download of full archive. Sit tight, this can take about 20 minutes!")
        min_date_list = pd.date_range(start_date, today, freq='MS').strftime('%Y-%m-%d').tolist()
        max_date_list = pd.date_range(start_date, next_month, freq='M').strftime('%Y-%m-%d').tolist()
        # max_date_list.append(next_month_str)

        # List for all the PQS to go in:
        master_wpqs = []

        # Iterate through idx, get unanswered wpqs from each month and append to the master_wpqs list
        for i, e in enumerate(tqdm(max_date_list, leave=False)):
            lst_wpqs = get_wpqs_by_date(tabledWhenFrom=min_date_list[i], tabledWhenTo=max_date_list[i])
            for x in lst_wpqs:
                master_wpqs.append(x) 

        # Convert to DataFrame
        pqs = pd.DataFrame(master_wpqs)
        pqs.drop(columns=['attachments', 'groupedQuestions', 'groupedQuestionsDates'], inplace=True)
        pqs.drop(columns=[
                'isWithdrawn',
                'isNamedDay',
                'answerIsHolding',
                'answerIsCorrection',
                'answeringMemberId',
                'answeringMember',
                'correctingMemberId',
                'correctingMember',
                'dateAnswered',
                'answerText',
                'originalAnswerText',
                'comparableAnswerText',
                'dateAnswerCorrected',
                'dateHoldingAnswer',
                'attachmentCount',
            ], inplace=True)
        # pqs.drop_duplicates(inplace=True)
        pqs['dateTabled'] = pd.to_datetime(pqs.dateTabled)
        pqs.to_csv(Path(tmp+'/ua_pqs.csv'), index=False, index_label=False)
        print('Full archive downloaded up to {d}. To get WPQs tabled since that date, call this function once more.'.format(d=pqs.dateTabled.max().strftime('%Y-%m-%d')))
        return pqs