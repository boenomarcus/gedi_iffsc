import os
import sys
import pymongo
import time
import requests
import tkinter as tk

from subprocess import Popen
from getpass import getpass
from netrc import netrc
from tkinter import filedialog
from glob import glob
from datetime import datetime
from utils import strings, numbers, config, geoTasks, gediClasses

# Get ROI Shapely Polygon
roi_poly = geoTasks.shapelyPol_from_GeoJSONSinglePol(config.roiPath)


def gedi_finder():
    """
    > gedi_finder()
        GEDI Finder - Search LP_DAAC/NASA Database for new GEDI Granules.

    > Arguments:
        - No arguments.
    
    > Output:
        - Text file (.txt) with granules to download from EarthData Search.
    """
    while True:

        # GEDI Finder menu
        gediFinder_Menu = [
            f"Default Bounding Box ({config.default_bbox})",
            "New Bounding Box",
            "Return to Main Menu",
            "Exit System"
            ]
            
        # Print options of actions for the user to select 
        print("\n" + "- - " * 20)
        print("\n> Define Bounding Box for LP_DAAC/NASA search:\n")
        for pos, options in enumerate(gediFinder_Menu):
            print(
                "[{}] {}".format(
                    strings.colors(pos+1, 3), strings.colors(options, 2)
                    )
                )
            
        # Identifying next action
        gf_option = numbers.readOption(
            "Select an option: ", 
            len(gediFinder_Menu)
            )

        if gf_option == 1:
            # Ask for user confirmation
            answer = strings.yes_no_input(
                f"\nConfirm search with default bbox?! [(y)/n] "
                )

            # Test whether user confirmed search or not             
            if answer in "Yy":
                # Create LP_DAAC/NASA Request with default bbox
                gf_search(config.default_bbox)
                break
            else:
                print("\n ... Redefining terms of the search ...\n")
    
        elif gf_option == 2:
            # Define bbox
            usr_bbox = gf_bbox()

            # Ask for user confirmation
            answer = strings.yes_no_input(
                f"\nConfirm search with {usr_bbox} bbox?! [(y)/n] "
                )

            # Test whether user confirmed search or not             
            if answer in "Yy":
                # Create LP_DAAC/NASA Request with default bbox
                gf_search(usr_bbox)
                break
            else:
                print("\n ... Redefining terms of the search ...\n")   
               
        elif gf_option == 3:
            # Return to Main Menu
            print("\n >> Returning to Main Menu ...\n")
            print("\n" + "- - " * 20, "\n")
            break
        
        else:
            # Exit system with a goodbye message
            sys.exit("\n" + strings.colors("Goodbye, see you!", 1) + "\n")


def gedi_downloader():
    """
    > gedi_downloader()
        Function to download pre-processed GEDI Granules from LPDAAC Server.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs (function leads to GEDI Granules download).
    """
    # Set empty entries 
    source_links = "...empty..."
        
    while True:
        gediDownloader_Menu = [
            f"Define file with LPDAAC links (current: {source_links})",
            "Define subset and download files",
            "Return to Main Menu",
            "Exit System"
            ]
            
        # Print options of actions for the user to select
        print("\n" + "- - " * 20)
        print("\n> Download pre-processed GEDI Granules from LPDAAC Server:\n")
        for pos, options in enumerate(gediDownloader_Menu):
            print(
                "[{}] {}".format(
                    strings.colors(pos+1, 3), strings.colors(options, 2)
                    )
                )
        
        # Identifying next action
        downloader_option = numbers.readOption(
            "Select an option: ", 
            len(gediDownloader_Menu)
            )
        
        if downloader_option == 1:
            # Open up filedialog to the user select file with links to download
            root = tk.Tk()
            root.withdraw()
            source_links = filedialog.askopenfilename()
        
        elif downloader_option == 2:
            if source_links != "...empty...":
                
                # Retrieve list of granules to download, and define a subset
                files2down, beg, end = gd_files2down(source_links)
                
                if len(files2down) > 0:

                    # Authenticate EarthData login to request downloads
                    print("\n ... Connecting with LPDAAC_NASA Server ...")
                    time.sleep(2)
                    # Checking credentials
                    gd_check_credentials()

                    # Download files
                    print("\n ... Starting Download Routine ...\n")
                    print(f"Files to download: {end-beg}\n")
                    gd_download(files2down, beg, end)
                    print(f"\n ...  All files were succesfully downloaded! ...\n")
                    break
                
                else:
                    print(
                        strings.colors(
                            f"\n ...  All files in link list are already downloaded! ...\n", 3
                            )
                        )
                    print("\n" + "- - " * 20, "\n")
                    break

            else:
                # Ask user to define links source file prior to resume download
                if source_links == "...empty...":
                    print(
                        strings.colors(
                            f"> Define Links Source File [Option 1]", 1
                            )
                        )
             
        elif downloader_option == 3:
            # Go to GEDI Extractor Menu
            # Return to Main Menu
            print("\n >> Returning to main menu ...\n")
            print("\n" + "- - " * 20, "\n")
            break

        else:
            # Exit system with a goodbye message
            sys.exit("\n" + strings.colors("Goodbye, see you!", 1) + "\n")


def gedi_storer():
    """
    > gedi_storer()
        Function to update gedi database on MongoDB.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs (function leads to MongoDB update).
    """

    # Get local storage files
    files = gs_get_files()

    # Get gedi versions inside local storage
    versions = gs_get_versions(files)

    # Get dictionary with files per version and product level
    files_dict = gs_match_files(files, versions)

    # Get dictionary of files to process
    final_files, numgranules = gs_files_to_Process(files_dict)

    if numgranules == 0:
        print(
            strings.colors(
                f"\n'{config.base_mongodb}' is already up-to-date!",
                3
                )
            )
        print("\n" + "- - " * 20 + "\n")

    else:

        # Print number of files to process
        print(strings.colors(f"\nUpdating {numgranules} GEDI Granules", 2))

        # Get dictionary keys
        for version in list(final_files.keys()):
            for match in list(final_files[version].keys()):

                if len(final_files[version][match]) < 3:
                    print(f"\nDownload all '{match}' Granules to continue!")
                    print("... Moving to the next GEDI Granule ...\n")
                else:

                    # Create class instance to process shots
                    gediShots = gediClasses.GEDI_Shots(
                        path = config.localStorage,
                        l1b = final_files[version][match][0],
                        l2a = final_files[version][match][1],
                        l2b = final_files[version][match][2],
                        vers = version,
                        strMatch = match,
                        beams = config.beam_list,
                        db = config.base_mongodb,
                        extent = roi_poly
                    )

                    # Process shots and store into MongoDB
                    gediShots.process_and_store()

                    # Update process log
                    gediShots.update_process_log()
        
        print(
            strings.colors(
                f"\n > MongoDB '{config.base_mongodb}' succesfully updated!", 
                2
                )
            )
        print("\n" + "- - " * 20 + "\n")


def gedi_extractor():
    pass


# ----- GEDI Finder methods -------------------------------------------------- #

def gf_bbox():
    """
    > gf_bbox():
        GEDI Finder method - Create user-define bounding box.

    > Arguments:
        - No arguments.
    
    > Output:
        - list: bounding box [ul_lat, ul_lon, lr_lat, lr_lon].
    """
    while True:
        print("\n" + "--- Enter Bounding Box Coordinates [WGS84 lat/long]:")
        coords = [
            "Upper-Left Latitude: ",
            "Upper-Left Longitude: ",
            "Lower-Right Latitude: ",
            "Lower-Right Longitude: "
            ]
        bbox = [
            numbers.readFloat(strings.colors(coords[i], 2)) for i in range(4)
        ]

        # Check if it is a valid bounding box
        if bbox[0] > bbox[2] and bbox[1] < bbox[3]:
            # Return usr_bbox if it is valid
            return bbox
        print(strings.colors("[ERROR] Enter a valid Bounding Box!", 1))


def gf_search(bbox):
    """
    > gf_bbox():
        Create a GEDI Finder request.

    > Arguments:
        - bbox: Bounding Box for LP_DAAC/NASA query.
    
    > Output:
        - Text file (.txt) with granules to download from EarthData Search.
    """
    print('\n ... Delivering GEDI Finder Request to LP_DAAC/NASA Server ...')

    # Retrieve info on products and versions
    products = config.gedi_products
    versions = config.gedi_versions

    # Create list of products and versions
    pv_list = [[prod, version] for prod in products for version in versions] 

    # Create empty list to store results
    gedi_granules = []

    # Iterate over list of products and versions
    for product, version in pv_list:
        request = gediClasses.GEDI_request(p=product, v=version, bbox=bbox)
        link_list = request.process_request()

        # Get only filenames, not entire URL
        link_list = [f[57:] for f in link_list]

        # Update list
        gedi_granules.extend(link_list)
    
    # Create empty list to store results
    gedi_granules_all_files = []
    gedi_granules_to_download = []

    # Sometimes v02 products are inside v01 products on LP_DAAC/NASA Server
    # so we need to check for this bug 
    prods_corr = sorted(list(set([f[0:8] for f in gedi_granules])))
    vers_corr = sorted(list(set([f[-5:-3] for f in gedi_granules])))

    # Update pv_list with corrected versions
    pv_list = [[prod, version] for prod in prods_corr for version in vers_corr]
    
    # Check for products already downloaded
    for index, item in enumerate(pv_list):
        
        # Retrieve files that match product and version
        beg, end = item[0], item[1] + ".h5"
        gedi_granules_all_files.append(
            [f for f in gedi_granules if f.startswith(beg) and f.endswith(end)]
        )

        # files on storage
        folder = config.localStorage + os.sep + item[0] + os.sep
        files_localStorage = [
            os.path.basename(f)[10:] for f in glob(
                folder + "*" + item[1] + ".h5"
                )
            ]
        
        # Files to download
        gedi_granules_to_download.append(
            sorted(
                list(
                    set(gedi_granules_all_files[index]) - set(files_localStorage)
                )
                )
            )
    
    # Create text file with results
    print("\n ... Requested Successfully Completed ...")
    
    # Save text file with results
    gf_write_searchResults(
        bbox=bbox, 
        prodVers_list=pv_list, 
        full_list=gedi_granules_all_files, 
        toDownload_list=gedi_granules_to_download
        )

    print("\n ... Saving GEDI Granules to a text file ('.txt') ...")
    time.sleep(2)
    print("\n" + "- - " * 20 + "\n")


def gf_write_searchResults(bbox, prodVers_list, full_list, toDownload_list):
    """
    > gf_write_searhResults(full_list, toDownload_list):
        Create a GEDI Finder request.

    > Arguments:
        - bbox: Bounding box defined by the user;
        - prodVers_list: Nested list indicating GEDI products and versions;
        - full_list: Nested lists with all matching GEDI Granules;
        - toDownload_list: Nested lists with GEDI Granules to download.
    
    > Output:
        - No outputs (function leads to writing of text file).
    """

    # Creating output file name
    dt = datetime.now()
    ymd = str(dt.now().year).zfill(4) + str(dt.now().month).zfill(2)
    ymd += str(dt.now().day).zfill(2)
    hms = str(dt.now().hour).zfill(2) + str(dt.now().minute).zfill(2)
    hms += str(dt.second).zfill(2)
    #
    out_file = "gedi_finder_links" + os.sep + "GEDI_" + ymd + "_" + hms
    out_file += "_bbox_"
    out_file += "_".join([str(int(n)) for n in bbox]) + ".txt"

    # Create results directory if it does not exist 
    if not os.path.exists("gedi_finder_links"):
        os.makedirs("gedi_finder_links")

    # Save resutls to out_file
    with open(out_file, "a+") as f:

        f.write("\n# --- GEDI Finder Results\n")
        f.write(f"User-defined Bounding Box: {bbox}\n\n")
        
        f.write("# --- GRANULES TO DOWNLOAD\n\n")
        for index, item in enumerate(prodVers_list):
            f.write(f"\n# - Product: {item[0]} - Version: {item[1]}\n\n")
            
            # Writing granules to text file
            list_lenght = len(toDownload_list[index])
            for pos, gedi_file in enumerate(toDownload_list[index]):
                if pos == list_lenght - 1:
                    f.write(f"{gedi_file}\n")
                else:
                    f.write(f"{gedi_file},\n")
        
        f.write("\n\n# --- ALL GRANULES\n\n")
        for index, item in enumerate(prodVers_list):
            f.write(f"\n# - Product: {item[0]} - Version: {item[1]}\n\n")
            
            # Writing granules to text file
            list_lenght = len(full_list[index])
            for pos, gedi_file in enumerate(full_list[index]):
                if pos == list_lenght - 1:
                    f.write(f"{gedi_file}\n")
                else:
                    f.write(f"{gedi_file},\n")


# ----- GEDI Downloader methods ---------------------------------------------- #


def gd_files2down(src_file):
    """
    > gd_files2down(src_file)
        Function to retrieve files to be downloaded.

    > Arguments:
        - src_file: Text file containing links for pre-processed GEDI granules
 
    > Output:
        - beg: ;
        - end: ;
        - finalList: .
    """
    
    # Get list of granules to download
    fileList = []
    with open(src_file, "r") as f:
        lines = f.readlines()
        for item in lines:
	        fileList.append(item.rstrip("\n"))
    
    # Retrieve GEDI Products
    prods = sorted(list(set([f[-49:-41] for f in fileList])))
    
    # Iterate over GEDI Product
    files2down = []
    files_alrdDown = []
    
    for prod in prods:

        # Create product local storage directory if they do not exist
        if not os.path.exists(config.localStorage + os.sep + prod):
            
            # Create local storage
            os.makedirs(config.localStorage + os.sep + prod)
            
            # Get all files for that product
            files2down.extend([f for f in fileList if f[-49:-41] == prod])
        
        else:
            # Get list of files already downloaded
            filesDown = [
                os.path.basename(f) for f in glob(
                    config.localStorage + os.sep + prod + os.sep + "*.h5"
                    )
            ]

            # Update list of files to download 
            if len(filesDown) > 0:
                prodFiles = [f for f in fileList if f[-49:-41] == prod]
                files2down.extend([f for f in prodFiles if f[-59:] not in filesDown])
                files_alrdDown.extend([f for f in prodFiles if f[-59:] in filesDown])

            else:
                files2down.extend([f for f in fileList if f[-49:-41] == prod])
    
    if len(files2down) == 0:

        # Print info on number of granules to be downloaded
        print("\n" + "- - " * 20)
        print(f"\nLPDAAC_NASA Download links (Granules): {len(fileList)}")
        print(f"Files already downloaded: {len(files_alrdDown)}")
        print(f"Files to download: {len(files2down)}\n")
        
        # Return nulls if there are no files to download
        return files2down, None, None
    
    else:
    
        while True:

            # Print info on number of granules to be downloaded
            print("\n" + "- - " * 20)
            print(f"\nLPDAAC_NASA Download links (Granules): {len(fileList)}")
            print(f"Files already downloaded: {len(files_alrdDown)}")
            print(f"Files to download: {len(files2down)}\n")

            # Ask if user wants to download a subset of the files
            gediDownloader_subsetMenu = [
                    "Download them all",
                    "Create a subset",
                    "Return to Main Menu",
                    "Exit System"
                    ]
                    
            # Print options of actions for the user to select 
            print("\n> Select an option to download files:\n")
            for pos, options in enumerate(gediDownloader_subsetMenu):
                print(
                    "[{}] {}".format(
                        strings.colors(pos+1, 3), strings.colors(options, 2)
                        )
                    )
                
            # Identifying next action
            downloaderSubset_option = numbers.readOption(
                "Select an option: ", 
                len(gediDownloader_subsetMenu)
                )
            
            if downloaderSubset_option == 1:
                # Download all files
                beg = 0
                end = len(files2down)
                
            elif downloaderSubset_option == 2:
                # Ask user to enter a subset
                print(f"\nFirst index: {0} - Last index: {len(files2down)-1}")
                beg = numbers.readListIndex("Begin index: ", 0, len(files2down))
                end = numbers.readListIndex("End index: ", beg+1, len(files2down))
                
            elif downloaderSubset_option == 3:
                # Return to Main Menu
                print("\n >> Returning to main menu ...\n")
                print("\n" + "- - " * 20, "\n")
                break
                
            else:
                # Exit system with a goodbye message
                sys.exit("\n" + strings.colors("Goodbye, see you!", 1) + "\n")
            
            # Ask for user confirmation
            answer = strings.yes_no_input(
                f"\nConfirm file subset as [{beg}:{end}] {end-beg} files?! [(y)/n] "
                )

            # Test whether user confirmed search or not             
            if answer in "Yy":
                # Return results
                return files2down, beg, end
                break
            else:
                print("\n ... Redefining subset ...\n")
        

def gd_download(files2down, beg, end):

    # Loop through and download all files
    fileList = files2down[beg:end]
    list_length = len(fileList)

    # Retrieve GEDI Products
    prods = sorted(list(set([f[-49:-41] for f in fileList])))

    # Create and submit request and download file
    netrcDir = os.path.expanduser("~/.netrc")
            
    # Address to call for authentication
    urs = 'urs.earthdata.nasa.gov'

    # Iterate over gedi products
    file_counter = 0
    for prod in prods:
        
        # Get files of the given product
        prodFiles = [f for f in fileList if f[-49:-41] == prod]

        # Defining output folder (see config.localStorage)
        outFolder = config.localStorage + os.sep + prod

        # Iterate over files
        for f in prodFiles:
            
            # Create filename
            fileName = os.path.join(outFolder, f.split('/')[-1].strip())

            # Increase file counter
            file_counter += 1

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
                    print("\n{} not downloaded. Verify username/password\n".format(
                            f.split('/')[-1].strip()
                            )
                        )
                else:
                    response.raw.decode_content = True
                    content = response.raw
                    with open(fileName, 'wb') as d:
                        print("\nDownloading file {} of {}:\n   > {}...\n".format(
                                file_counter, 
                                list_length, 
                                f
                                )
                            )
                        while True:
                            chunk = content.read(16 * 1024)
                            if not chunk:
                                break
                            d.write(chunk)
                    print(strings.colors(f"   > [DONE] File Succesfully downloaded\n", 2))

        


def gd_check_credentials():
    
    # Address to call for authentication
    urs = "urs.earthdata.nasa.gov"

    prompts = [
        'Enter NASA Earthdata Login Username: ',
        'Enter NASA Earthdata Login Password: '
        ]
    
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


# ----- GEDI Storer methods -------------------------------------------------- #


def gs_get_files(path=config.localStorage):
    """
    > gs_get_files(path=config.localStorage)
        Function to get files into local storage (see utils/config.py).

    > Arguments:
        - path: Path to local folder with downloaded GEDI Granules.
            --> default = config.localStorage (see utils/config.py)
    
    > Output:
        - List of granules in local storage (all gedi versions and levels).
    """
    # Start empty list to store results
    files = []

    # Iterate through product list
    for product in config.gedi_products:
        files.extend(
            [
                os.path.basename(f) for f in glob(
                      path + os.sep + product + os.sep + "*.h5"
                    )
                ]
            )
    
    # Return results
    return files


def gs_get_versions(files):
    """
    > gs_get_versions(files)
        Function to get versions of GEDI Granules in local storage.

    > Arguments:
        - files: list of GEDI filenames.
    
    > Output:
        - List of GEDI Versions in local storage.
    """
    return list(set([v[-5:-3] for v in files]))


def gs_match_files(files, versions):
    """
    > gs_match_files(files, versions)
        Function to get dictionary of matching L1B, L2A and L2B Granules.

    > Arguments:
        - files: list of GEDI filenames;
        - versions: list of GEDI Versions.
    
    > Output:
        - Dictionary of matching granules by version.
    """
    
    # Create empty dictionary to store results
    gedi_dict = {}

    # iterate through versions
    for version in versions:
        
        # Create nested dictionary for version
        gedi_dict[version] = {}

        # Get files for version
        version_files = [f for f in files if f.endswith(version + ".h5")]
    
        # Get L1B files for version
        l1b_files = [
            f for f in version_files if f.startswith("processed_GEDI01_B")
            ]
        
        # Iterate through L1B files
        for l1b_file in l1b_files:
            
            # Get match parameter
            str2match = l1b_file[19:46]

            # Get matching L2A and L2B files
            matched_files = [f for f in files if f[19:46] == str2match]

            # Append files to dictionary
            gedi_dict[version][str2match] = matched_files
    
    # Return results
    return gedi_dict


def gs_files_to_Process(files_dict):
    """
    > gs_files_to_Process(files_dict)
        Function to get dictionary of GEDI Granules to process.

    > Arguments:
        - files_dict: Dictionary of matching granules by version.
            --> Output from gs_match_files().
    
    > Output:
        - final_dict: Dictionary of granules to process;
        - granules: Number of granules to process.
    """
    # Empty dictionary to store results
    final_dict = {}

    # Reckon versions
    versions = list(files_dict.keys())

    # Create MongoDB Connection
    with pymongo.mongo_client.MongoClient() as mongo:

        # Acces database
        db = mongo.get_database(config.base_mongodb)
        
        # iterate through versions
        for version in versions:

            # Create nested dictionary for version
            final_dict[version] = {}

            # Log collection name
            collec_name = "processed_v" + version

            # Test whether log exists or not
            if collec_name in db.list_collection_names():

                # Iterate over matching files
                for match in list(files_dict[version].keys()):
                    
                    # Retrieve processed files
                    processed_files = db[collec_name].find_one({
                        "str2match": match
                    })

                    # If no matches found, processed_files wil be a NoneType
                    if processed_files is None:
                        final_dict[version][match] = files_dict[version][match]
            else:
                # If log does not exist, process all files
                final_dict[version] = files_dict[version]
    
    # Get number of GEDI granules to process
    granules = 0
    for version in versions:
        granules += len(list(final_dict[version].keys()))
    
    # Return results
    return final_dict, granules


# ----- GEDI Extractor methods ----------------------------------------------- #