#!/usr/bin/python

import os
from wpqs import update_answered_pqs, download_ua_pqs

def initialise_dir():
    MYDIR = ('tmp')
    CHECK_FOLDER = os.path.isdir(MYDIR)

    if not CHECK_FOLDER:
        os.makedirs(MYDIR)
        print('Created a temporary folder to hold the data')
        return MYDIR, True
    else:
        print(MYDIR, "folder already exists.")
        return MYDIR, False

    return MYDIR

if __name__ == '__main__':
    # Sort out filesystem
    dir, t = initialise_dir()
    # update archive of answered WPQs. Each function called 
    update_answered_pqs(tmp=dir)
    if t:
        print('Running again to make sure everything is up-to-date.')
        update_answered_pqs(tmp=dir)
    else:
        pass
    download_ua_pqs(tmp=dir)
    if t:
        print('Running again to make sure everything is up-to-date.')
        download_ua_pqs(tmp=dir)
    else:
        pass