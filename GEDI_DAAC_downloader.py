"""
 How to Access the LP DAAC Data Pool with Python
 The following Python code example demonstrates how to configure a 
 connection to download data from an Earthdata Login enabled server,
 specifically the LP DAAC's Data Pool.
"""

import argparse
import time
import os
import requests

from subprocess import Popen
from getpass import getpass
from netrc import netrc

# ---------------USER-DEFINED VARIABLES--------------------------------------- #
# Set up command line arguments
parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

parser.add_argument(
    '-dir', 
    '--directory', 
    required=True, 
    help='Specify directory to save files to'
    )

parser.add_argument(
    '-f', 
    '--files', 
    required=True, 
    help='A single granule URL, or the location of csv or txt with granule URLs'
    )

args = parser.parse_args()

# Set local directory to download to
saveDir = args.directory 

# Define file(s) to download from the LP DAAC Data Pool
files = args.files        
prompts = [
    'Enter NASA Earthdata Login Username: ',
    'Enter NASA Earthdata Login Password: '
    ]

# ---------------------------------SET UP WORKSPACE---------------------------------------------- #

# Create a list of files to download based on input type of files above
if files.endswith('.txt') or files.endswith('.csv'):
    # If input is textfile with file URLs
    fileList = open(files, 'r').readlines()

elif isinstance(files, str):
    # If input is a single file
    fileList = [files]

# Generalize download directory
if saveDir[-1] != '/' and saveDir[-1] != '\\':
    saveDir = saveDir.strip("'").strip('"') + os.sep

# Address to call for authentication
urs = 'urs.earthdata.nasa.gov'

# --------------------------------AUTHENTICATION CONFIGURATION----------------------------------- #

# Determine if netrc file exists, and if so, if it includes NASA Earthdata Login Credentials
try:
    netrcDir = os.path.expanduser("~/.netrc")
    netrc(netrcDir).authenticators(urs)[0]

# Below, create a netrc file and prompt user for NASA Earthdata Login Username and Password
except FileNotFoundError:
    homeDir = os.path.expanduser("~")
    Popen('touch {0}.netrc | chmod og-rw {0}.netrc | echo machine {1} >> {0}.netrc'.format(homeDir + os.sep, urs), shell=True)
    Popen('echo login {} >> {}.netrc'.format(getpass(prompt=prompts[0]), homeDir + os.sep), shell=True)
    Popen('echo password {} >> {}.netrc'.format(getpass(prompt=prompts[1]), homeDir + os.sep), shell=True)

# Determine OS and edit netrc file if it exists but is not set up for NASA Earthdata Login
except TypeError:
    homeDir = os.path.expanduser("~")
    Popen('echo machine {1} >> {0}.netrc'.format(homeDir + os.sep, urs), shell=True)
    Popen('echo login {} >> {}.netrc'.format(getpass(prompt=prompts[0]), homeDir + os.sep), shell=True)
    Popen('echo password {} >> {}.netrc'.format(getpass(prompt=prompts[1]), homeDir + os.sep), shell=True)

# Delay for up to 1 minute to allow user to submit username and password before continuing
tries = 0
while tries < 30:
    try:
        netrc(netrcDir).authenticators(urs)[2]
    except:
        time.sleep(2.0)
    tries += 1

# -----------------------------------------DOWNLOAD FILE(S)-------------------------------------- #

# Loop through and download all files to the directory specified above
# Get numbe of files to download
list_length = len(fileList)

for index, f in enumerate(fileList):
    
    # Create path and savename
    if not os.path.exists(saveDir):
        os.makedirs(saveDir)
    saveName = os.path.join(saveDir, f.split('/')[-1].strip())

    # Create and submit request and download file
    with requests.get(
        f.strip(), 
        verify=False, 
        stream=True, 
        auth=(
            netrc(netrcDir).authenticators(urs)[0], 
            netrc(netrcDir).authenticators(urs)[2])
        ) as response:
        
        # Check for login credentials issues
        if response.status_code != 200:
            print("\n{} not downloaded. Verify username/password in {}\n".format(
                    f.split('/')[-1].strip(), netrcDir
                    )
                )
        else:
            response.raw.decode_content = True
            content = response.raw
            with open(saveName, 'wb') as d:
                print(f"\nDownloading file {} of {}:\n   > {}...\n".format(
                        index+1, 
                        list_length, 
                        f
                        )
                    )
                while True:
                    chunk = content.read(16 * 1024)
                    if not chunk:
                        break
                    d.write(chunk)
            print(f"File Succesfully downloaded: {saveName} [DONE]\n")
    
