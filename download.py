#!/usr/bin/python

import os
from wpqs import update_answered_pqs, download_ua_pqs

def initialise_dir():
    MYDIR = ('tmp')
    CHECK_FOLDER = os.path.isdir(MYDIR)

    if not CHECK_FOLDER:
        os.makedirs(MYDIR)
        print('Created a temporary folder to hold the data')
    else:
        print(MYDIR, "folder already exists.")

    return MYDIR

if __name__ == '__main__':
    # Sort out filesystem
    dir = initialise_dir()
    # update archive of answered WPQs. Each function called 
    update_answered_pqs(tmp=dir)
    update_answered_pqs(tmp=dir)
    download_ua_pqs(tmp=dir)
    download_ua_pqs(tmp=dir)