"""
GEDI Tasks utilities

Functions to search, download, store and extract GEDI Shots data from
MongoDB Database

Author: Marcus Moresco Boeno

"""

# Standard library imports
import os
import sys
import time
import requests
import tkinter as tk

# library specific imports
from subprocess import Popen
from getpass import getpass
from netrc import netrc
from tkinter import filedialog
from glob import glob
from datetime import datetime

# Third party library imports
import pymongo
import geopandas as gpd

# Local application imports
from utils import strings, numbers, config, geoTasks, gediClasses


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
            "Download files",
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
            # Open up filedialog to source file selection
            root = tk.Tk()
            root.withdraw()
            source_links = filedialog.askopenfilename()
        
        elif downloader_option == 2:
            if source_links != "...empty...":
                
                # Retrieve list of granules to download, and define a subset
                files2down = gd_files2down(source_links)
                
                if len(files2down) > 0:

                    # Authenticate EarthData login to request downloads
                    print("\n ... Retrieve LPDAAC_NASA User Credentials ...")
                    time.sleep(1.5)
                    # Checking credentials
                    gd_check_credentials()

                    # Download files
                    print("\n ... Starting Download Routine ...\n")
                    time.sleep(1.5)
                    gd_download(files2down)
                    print(f"\n ...  Download Routine Completed! ...\n")
                    print("\n" + "- - " * 20, "\n")
                    break
                
                else:

                    # Informe user that all files are already downloaded
                    msg = "All files in link list are already downloaded!"
                    print(strings.colors(f"\n ... {msg} ...\n", 3))
                    print("\n" + "- - " * 20, "\n")
                    break

            else:
                # Ask user to define links source file prior to resume download
                print(strings.colors("> Define Links Source File!", 1))
                print("\n" + "- - " * 20, "\n")
             
        elif downloader_option == 3:
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
    all_files = gs_get_files()

    # Get gedi versions inside local storage
    versions = gs_get_versions(all_files)

    # Get dictionary with files per version and product level
    files_dict = gs_match_files(all_files, versions)

    # Get dictionary of files to process
    files, numgranules = gs_files_to_Process(files_dict)

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
        for version in list(files.keys()):
            for index_gran, match in enumerate(list(files[version].keys())):

                if len(files[version][match]) < 3:
                    print(f"\nDownload all '{match}' Granules to continue!")
                    print("... Moving to the next GEDI Granule ...\n")
                else:

                    # Create class instance to process shots
                    gediShots = gediClasses.GEDI_Shots(
                        path = config.localStorage,
                        l1b = files[version][match][0],
                        l2a = files[version][match][1],
                        l2b = files[version][match][2],
                        vers = version,
                        strMatch = match,
                        beams = config.beam_list,
                        db = config.base_mongodb,
                        extent = gpd.read_file(config.roiPath)["geometry"][0],
                        index_gran=index_gran+1, 
                        num_grans=numgranules
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
    """
    > gedi_extractor()
        Function to extract GEDI Shot data from MongoDB instance.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs (function leads to GEDI Shot data extraction).
    """
    # Set empty entries 
    geometry_src = "...empty..."
    buffer = 0
    output_folder = "...empty..."
    output_format = "geojson"
    info2extract = "BASIC Shot Data (config.basicInfo)"
        
    while True:
        gediExtractor_Menu = [
            f"Define geometry (current: {geometry_src})",
            f"Define buffer [m] (current: {buffer})",
            f"Define output folder (current: {output_folder})",
            f"Define output format (current: {output_format})",
            f"Define info to extract (current: {info2extract})",
            "Extract GEDI Shots data",
            "Return to Main Menu",
            "Exit System"
            ]
            
        # Print options of actions for the user to select
        print("\n" + "- - " * 20)
        print("\n> GEDI Extractor menu:\n")
        for pos, options in enumerate(gediExtractor_Menu):
            print(
                "[{}] {}".format(
                    strings.colors(pos+1, 3), strings.colors(options, 2)
                    )
                )
        
        # Identifying next action
        extractor_option = numbers.readOption(
            "Select an option: ", 
            len(gediExtractor_Menu)
            )
        
        if extractor_option == 1:
            # Open up filedialog to source geometry file
            root = tk.Tk()
            root.withdraw()
            geometry_src = filedialog.askopenfilename()

            # Check if GeoJSON or ESRI Shapefile
            if geometry_src.split(".")[-1] not in ["geojson", "shp"]:
                print(
                    strings.colors(
                        "\n[ERROR] Select GeoJSON or ESRI Shapefile!",
                        1
                        )
                    )
                geometry_src = "...empty..."
                
        elif extractor_option == 2:
            while True:
                # Define buffer in meters
                buffer = numbers.readFloat("\nEnter a buffer, in meters: ")

                # Check if buffer is valid
                if buffer >=0:
                    break
                else:
                    print(strings.colors("[ERROR] Enter a positive buffer!", 1))
        
        elif extractor_option == 3:
            # Open up filedialog to select output folder
            root = tk.Tk()
            root.withdraw()
            output_folder = filedialog.askdirectory()
        
        elif extractor_option == 4:
            # Define output format
            formats = ["geojson", "shp", "csv"]

            # Print format options for the user to select
            print("\n> Output formats:\n")
            for pos, options in enumerate(formats):
                print(
                    "[{}] {}".format(
                        strings.colors(pos+1, 3), strings.colors(options, 2)
                        )
                    )
            
            # Ask user option
            format_option = numbers.readOption(
                "Select an option: ", 
                len(formats)
                )
            
            # Define output format
            output_format = formats[format_option-1]
        
        elif extractor_option == 5:
            # Define output format
            infoList = [
                "BASIC Shot Data (config.basicInfo)", 
                "FULL Shot Data (config.fullInfo)"]

            # Print format options for the user to select
            print("\n> GEDI Shots information to extract:\n")
            for pos, options in enumerate(infoList):
                print(
                    "[{}] {}".format(
                        strings.colors(pos+1, 3), strings.colors(options, 2)
                        )
                    )
            
            # Ask user option
            info_option = numbers.readOption(
                "Select an option: ", 
                len(infoList)
                )
            
            # Define output format
            info2extract = infoList[info_option-1]
        
        elif extractor_option == 6:
            # Extract GEDI Shots data

            # Make sure source file and destination folder are set
            if geometry_src != "...empty..." and output_folder != "...empty...":

                # Ask for user confirmation
                print("\n> Defined parameters for GEDI Shots extraction:")
                print(f"  - Source geometry: {strings.colors(geometry_src, 3)}")
                print(f"  - Buffer: {strings.colors(buffer, 3)}")
                print(f"  - Output folder: {strings.colors(output_folder, 3)}")
                print(f"  - Output format: {strings.colors(output_format, 3)}")
                print(f"  - Info to extract: {strings.colors(info2extract, 3)}")
                answer = strings.yes_no_input(
                    f"\nConfirm extraction as described above?! [(y)/n] "
                    )

                # Test whether user confirmed search or not             
                if answer in "Yy":
                    
                    # Create extraction task
                    if info2extract == 'BASIC Shot Data (config.basicInfo)':
                        
                        pass
                        # ge_extract_basic_info(
                        #     geometry_src,
                        #     buffer,
                        #     output_folder,
                        #     output_format
                        #     )
                        
                    else:
                        pass
                        # ge_extract_full_info(
                        #     geometry_src,
                        #     buffer,
                        #     output_folder,
                        #     output_format
                        #     )
            else:
                # Ask user to define missing info
                if geometry_src == "...empty...":
                    print(
                        strings.colors(
                            "\nDefine geometry source file to continue ...\n",
                            1
                            )
                        )

                if output_folder == "...empty...":
                    print(
                        strings.colors(
                            "\nDefine output folder to continue ...\n",
                            1
                            )
                        )
                
        elif extractor_option == 7:
            # Return to Main Menu
            print("\n >> Returning to main menu ...\n")
            print("\n" + "- - " * 20, "\n")
            break

        else:
            # Exit system with a goodbye message
            sys.exit("\n" + strings.colors("Goodbye, see you!", 1) + "\n")

            


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
        - list: List of files (full links) to download.
    """
    
    # Get list of files
    fileList = [f.strip() for f in open(src_file, "r").readlines()]
    
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
    
    # Print info on number of granules to be downloaded
    print("\n" + "- - " * 20)
    print(f"\nLPDAAC_NASA Download links (Granules): {len(fileList)}")
    print(f"Files already downloaded: {len(files_alrdDown)}")
    print(f"Files to download: {len(files2down)}\n")
    
    # Return list of files to download
    return files2down
    
        

def gd_download(files2down):
    """
    > gedi_downloader()
        Function to download pre-processed GEDI Granules from LPDAAC Server.

    > Arguments:
        - files2down: List of files (full link) to download.
            ---> Output from gd_files2down()
    
    > Output:
        - No outputs (function leads to GEDI Granules download).
    """
    # Get number of files to download
    listLen = len(files2down)

    # Retrieve GEDI Products
    prods = sorted(list(set([f[-49:-41] for f in files2down])))

    # Create and submit request and download file
    netrcDir = os.path.expanduser("~/.netrc")
            
    # Address to call for authentication
    urs = 'urs.earthdata.nasa.gov'

    # Authentication credentials
    auth = (
        netrc(netrcDir).authenticators(urs)[0], 
        netrc(netrcDir).authenticators(urs)[2]
        )

    # Iterate over gedi products
    fileCount = 0
    for prod in prods:
        
        # Get files of the given product
        prodFiles = [f for f in files2down if f[-49:-41] == prod]

        # Defining output folder (see config.localStorage)
        outFolder = config.localStorage + os.sep + prod

        # Iterate over files
        for f in prodFiles:
            
            # Create filename
            fileName = os.path.join(outFolder, f.split('/')[-1].strip())

            # Increase file counter
            fileCount += 1

            try:
                # Try to establish connection and get GEDI Granule
                response = requests.get(f, verify=False, stream=True, auth=auth)  
            
            except ConnectionRefusedError:
                message = "[ConnectionRefusedError] - Connection denied"
                print(strings.colors(f"\n{message}\n", 1))
            
            else:

                # Check for code status
                if response.status_code != 200:
                    fn = f.split('/')[-1].strip()
                    print(strings.colors(f"\n{fn} not downloaded\n", 1))
                    print(f"   > Status Code: {response.status_code}")
                    print("         > Server maintenance downtime - Code: 503")
                    print("         > Others - Code: Various\n")

                # If OK status code (200) continue to download
                else:

                    # Get raw web content
                    response.raw.decode_content = True
                    content = response.raw
                    
                    # Open file and start writing
                    with open(fileName, 'wb') as d:

                        # Print info on granule being downloaded
                        print(f"\nDownloading {fileCount} of {listLen}:")
                        print(f"   > {f} ...")
                        
                        # Reading and writing data
                        while True:
                            chunk = content.read(16 * 1024)
                            if not chunk:
                                break
                            d.write(chunk)
                    
                    # Print indication when granule download is complete
                    msg = "   > [DONE] File Succesfully downloaded" 
                    print(strings.colors(f"{msg}\n", 2))


def gd_check_credentials():
    """
    > gd_check_credentials()
        Authenticate User username/password for LPDAAC Server.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs (function leads to LPDAAC Server authentication).
    """
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