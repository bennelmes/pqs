import numpy as np
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from tqdm import tqdm
import re
tqdm.pandas()

# A function to clean up question text
def question_cleaner(question):
    q = re.sub(r',(?=\S)|:', ', ', question)
    q = q.replace("to ask her majesty's government ", "to ask her majesty's government, ").replace("to ask her majesty’s government ", "to ask her majesty's government, ")
    q = q.replace(', and', ' and').replace('foreign, commonwealth and development affairs', 'foreign commonwealth and development affairs').replace('digital, culture, media', 'digital culture media').replace('business, energy and industrial', 'business energy and industrial')
    q = q.replace('levelling up, housing and', 'levelling up housing and').replace('environment, food and rural affairs', 'environment food and rural affairs').replace('culture, media and sport', 'culture media and sport').replace('business, innovation and skills', 'business innovation and skills')
    q = q.replace('digital, culture, media and sport', 'digital culture media and sport')
    q = q.replace('housing, communities and local government', 'housing communities and local government')
    q = q.replace(', representing the church commissioners', ' representing the church commissioners, ') 
    q = q.replace('to ask the chairman of committees ', 'to ask the chairman of committees, ')
    q = q.replace('to ask the leader of the house ', 'to ask the leader of the house, ')
    q = q.replace("to ask her majesty’s government", "to ask her majesty's government, ")
    q = q.replace("to ask the senior deputy speaker ", "to ask the senior deputy speaker, ")
    q = q.replace("her majesty's government ", "her majesty's government, ")
    q = q.replace("to ask the secretary of state for education ", "to ask the secretary of state for education, ")
    q = q.replace("to ask the secretary of state for defence ", "to ask the secretary of state for defence, ")
    q = q.replace("to ask the secretary of state for work and pensions ", "to ask the secretary of state for work and pensions, ")
    q = q.replace("to ask the secretary of state for environment food and rural affairs ", "to ask the secretary of state for environment food and rural affairs, ")
    q = q.replace("to ask the secretary of state for health ", "to ask the secretary of state for health, ")
    q = q.replace("foreign and commonwealth affairs ", "foreign and commonwealth affairs, ")
    q = q.replace("foreign commonwealth and development affairs ", "foreign commonwealth and development affairs, ")
    q = q.replace("the senior deputy speaker ", "the senior deputy speaker, ")
    q = q.replace("secretary of state for the home department,", "secretary of state for the home department, ")
    q = q.replace("to ask mr chancellor of the exchequer ", "to ask mr chancellor of the exchequer, ")
    q = q.replace("to ask the minister of the cabinet office ", "to ask the minister of the cabinet office, ")
    q = q.replace("to ask the minister for the cabinet office ", "to ask the minister for the cabinet office, ")
    q = q.replace("to ask the secretary of state for communities and local government ", "to ask the secretary of state for communities and local government, ")
    q = ' '.join(q.split(', ')[1:])
    cleaned_question = q
    return cleaned_question


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

    headers = {'Accept': 'text/plain'}


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
        start_date = pqs.dateAnswered.max()#.strftime("%Y-%m-%d")
        # start_date = pd.to_datetime('2022-01-15')
        print("File pqs.csv found with {n} answered PQs in, last updated {d}. Looking for new PQs...".format(n=no_pqs, d=start_date.strftime("%Y-%m-%d")))

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

        new_pqs = pd.concat([pqs, n_pqs])
        new_pqs['dateAnswered'] = pd.to_datetime(new_pqs.dateAnswered)
        new_pqs = new_pqs.drop_duplicates()

        new_pqs.to_csv(Path(tmp+'/pqs.csv'), index=False, index_label=False)

        # This is an embarrassingly bad fix for a problem with dropping duplicates. 
        # Essentially, mixed datatypes prevented drop_duplicates() from working... that meant that I couldn't get an accurate read on how many extra PQs were being downloaded. 
        # This is a poor fix... oh well. 
        pqs = pd.read_csv(Path(tmp+'/pqs.csv'))
        pqs = pqs.drop_duplicates()
        pqs.to_csv(Path(tmp+'/pqs.csv'), index=False, index_label=False)
        new_length = pqs.shape[0]
        pqs_added = new_length - no_pqs
        print("Downloaded {n} new PQs, which have been added to the archive.".format(n=pqs_added))



    # Now handle situations where there's no file. 
    else:
        # If there's no file already downloaded, then we'll download everything since the beginning of time (May 2014, according to Parliament's API)
        print("No PQs archive found. Downloading full archive from scratch. Sit tight, this can take about 20 minutes!")
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
        pqs['dateAnswered'] = pd.to_datetime(pqs.dateAnswered)
        pqs.drop(columns=['attachments', 'groupedQuestions', 'groupedQuestionsDates'], inplace=True)
        pqs.drop_duplicates(inplace=True)
        pqs.to_csv(Path(tmp+'/pqs.csv'), index=False, index_label=False)
        print('Full archive downloaded up to {d}. To ensure your archive is completely up-to-date, it is recommended to call this function once more.'.format(d=pqs.dateAnswered.max().strftime('%Y-%m-%d')))

    print('Cleaning data...')

    wpqs = pd.read_csv(Path(tmp+'/pqs.csv'))
    wpqs['dateTabled'] = pd.to_datetime(wpqs.dateTabled)
    wpqs['heading'] = wpqs.heading.fillna('')
    # wpqs = wpqs[['id', 'askingMemberId', 'askingMember', 'house', 'dateTabled', 'questionText', 'answeringBodyName', 'heading']]

    # Populate a column with party appreviation in the WPQs database, if the source data is available. 
    try:
        active_p = pd.read_csv(Path(tmp+'/active_members.csv'))
        former_p = pd.read_csv(Path(tmp+'/former_members.csv'))

        all_p = pd.concat([active_p, former_p])
        all_p = all_p[['id', 'nameListAs', 'gender', 'latestPartyabbreviation']]

        id_party_dict = dict(zip(all_p.id, all_p.latestPartyabbreviation))
        wpqs['latestPartyabbreviation'] = wpqs.askingMemberId.progress_apply(lambda x: id_party_dict[x] if x in id_party_dict.keys() else 'n/a')
    except:
        pass

    # Make some of the string fields lower case to improve comparability and searchability
    wpqs['heading'] = wpqs.heading.progress_apply(lambda x: x.lower())
    wpqs['questionText'] = wpqs.questionText.progress_apply(lambda x: x.lower())

    # Sometime the heading is a generic topic, other times it's specified by a ":" symbol. We'll extract this into a 'topic' column.
    wpqs['topic'] = wpqs.heading.progress_apply(lambda x: x.split(':')[0])

    wpqs['year_month'] = wpqs.dateTabled.dt.to_period('M')
    wpqs['cleanedQuestion'] = wpqs.questionText.progress_apply(lambda x: question_cleaner(x))
    wpqs.to_csv('pqs_cleaned.csv', index=False, index_label=False)
    print('Cleaning done. Output saved in ')

    return wpqs
    
    
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
        'answered': 'Any'
        }

    headers = {'Accept': 'text/plain'}

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
        print("File ua_pqs.csv found with {n} unanswered PQs in, last updated {d}. Looking for new PQs...".format(n=no_pqs, d=start_date.strftime("%Y-%m-%d")))

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
        print("Downloaded {n} new PQs, which have been added to the archive.".format(n=pqs_added))
        new_pqs.to_csv(Path(tmp+'/ua_pqs.csv'), index=False, index_label=False)
        # print("All done, be on your merry way.")


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

        pqs['dateTabled'] = pd.to_datetime(pqs.dateTabled)
        pqs['dateTabled'] = pqs.dateTabled.apply(lambda x: today if x > today else x)
        # pqs.drop_duplicates(inplace=True)
        pqs.to_csv(Path(tmp+'/ua_pqs.csv'), index=False, index_label=False)
        print('Full archive downloaded up to {d}. To ensure your archive is up-to-date, it is recommended to call this function once more.'.format(d=pqs.dateTabled.max().strftime('%Y-%m-%d')))


    print('Cleaning data...')

    wpqs = pd.read_csv(Path(tmp+'/ua_pqs.csv'))
    wpqs['dateTabled'] = pd.to_datetime(wpqs.dateTabled)
    wpqs['heading'] = wpqs.heading.fillna('')
    # wpqs = wpqs[['id', 'askingMemberId', 'askingMember', 'house', 'dateTabled', 'questionText', 'answeringBodyName', 'heading']]

    # Populate a column with party appreviation in the WPQs database, if the source data is available. 
    try:
        active_p = pd.read_csv(Path(tmp+'/active_members.csv'))
        former_p = pd.read_csv(Path(tmp+'/former_members.csv'))

        all_p = pd.concat([active_p, former_p])
        all_p = all_p[['id', 'nameListAs', 'gender', 'latestPartyabbreviation']]

        id_party_dict = dict(zip(all_p.id, all_p.latestPartyabbreviation))
        wpqs['latestPartyabbreviation'] = wpqs.askingMemberId.progress_apply(lambda x: id_party_dict[x] if x in id_party_dict.keys() else 'n/a')
    except:
        pass

    # Make some of the string fields lower case to improve comparability and searchability
    wpqs['heading'] = wpqs.heading.progress_apply(lambda x: x.lower())
    wpqs['questionText'] = wpqs.questionText.progress_apply(lambda x: x.lower())

    # Sometime the heading is a generic topic, other times it's specified by a ":" symbol. We'll extract this into a 'topic' column.
    wpqs['topic'] = wpqs.heading.progress_apply(lambda x: x.split(':')[0])

    wpqs['year_month'] = wpqs.dateTabled.dt.to_period('M')
    wpqs['cleanedQuestion'] = wpqs.questionText.progress_apply(lambda x: question_cleaner(x))
    wpqs.to_csv('ua_pqs_cleaned.csv', index=False, index_label=False)
    print('Cleaning done. Output saved in ')

    return wpqs