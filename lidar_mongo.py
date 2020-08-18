import os, sys, h5py, pymongo, geojson
import pandas as pd
import numpy as np
from datetime import datetime
from glob import glob
from shapely.geometry import Point, Polygon
from utils import classes, strings, numbers, config, geoTasks

# files per product

products = ['GEDI01_B', 'GEDI02_A', 'GEDI02_B']

# Get ROI Shapely Polygon 
roi_poly = geoTasks.shapelyPol_from_GeoJSONSinglePol(config.roiPath)


def main():
    """
    print greetings and main menu options
    """
    # Get local storage files
    files = retrieve_gedi_files()

    # Get gedi versions inside local storage
    versions = get_versions(files)

    # Get dictionary with files per version and product level
    files_dict = match_product_files(files, versions)

    # Get dictionary of files to process
    final_files, numgranules = return_files_to_process(files_dict)

    if numgranules == 0:
        print(
            strings.colors(
                f"\n'{config.base_mongodb}' is already up-to-date!\n",
                3
                )
            )
    else:

        # Print number of files to process
        print(strings.colors(f"\nUpdating {numgranules} GEDI Granules", 2))

        # Get dictionary keys
        for version in list(final_files.keys()):
            for match in list(final_files[version].keys()):

                if len(final_files[version][match]) < 3:
                    print(f"\nAll files (L1B, L2A and L2B) matchin '{match}' pattern must be download to continue!")
                    print("... Moving to the next GEDI Granule ...\n")
                else:

                    # Create class instance to process shots
                    gediShots = classes.GEDI_Shots(
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
                f"\n > Mongo database '{config.base_mongodb}' succesfully updated!", 
                2
                )
            )
    

def retrieve_gedi_files():
    """
    retrieve gedi files, all product levels
    """
    # Start empty list to store results
    files = []

    # Iterate through product list
    for product in products:
        files.extend(
            [
                os.path.basename(f) for f in glob(
                      config.localStorage + os.sep + product + os.sep + '*.h5'
                    )
                ]
            )
    
    # Return results
    return files

def get_versions(files):
    """
    retrieve gedi versions on local storage
    """
    return list(set([v[-5:-3] for v in files]))


def match_product_files(files, versions):
    """
    get dictionary with matching L1B, L2A and L2B files
    """
    
    # Create empty dictionary to store results
    gedi_dict = {}

    # iterate through versions
    for version in versions:
        
        # Create nested dictionary for version
        gedi_dict[version] = {}

        # Get files for version
        version_files = [f for f in files if f.endswith(version + '.h5')]
    
        # Get L1B files for version
        l1b_files = [
            f for f in version_files if f.startswith('processed_GEDI01_B')
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

def return_files_to_process(files_dict):
    """
    takes in a dictionary of files and return only those not processed yet
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
            collec_name = 'processed_v' + version

            # Test whether log exists or not
            if collec_name in db.list_collection_names():

                # Iterate over matching files
                for match in list(files_dict[version].keys()):
                    
                    # Retrieve processed files
                    processed_files = db[collec_name].find_one({
                        "str2match": match
                    })

                    if len(processed_files) == 0:
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

if __name__ == '__main__':
    
    # Print greetings message
    strings.greeting_lidar_mongo()

    while True:
        # Options for main menu
        mainMenu_options = [
            'Update GEDI Shots Database',
            'Update ICESat-2 (ATLAS) Database',
            'Update ICESat (GLAS) Database',
            'Exit system'
            ]
        
        # Print options of actions for the user to select 
        print('> Main Menu:\n')
        for pos, options in enumerate(mainMenu_options):
            print(
                '[{}] {}'.format(
                    strings.colors(pos+1, 3), strings.colors(options, 2)
                    )
                )
        
        # Identifying next action
        usr_option = numbers.readOption(
            'Select an option: ', 
            len(mainMenu_options)
            )
        
        # Either Update MongoDB or Exit System
        if usr_option == 1:
                
            # Ask for user confirmation
            answer = strings.yes_no_input(
                'Update MongoDB with new GEDI files?! [(y)/n] '
                    )
                
            if answer in 'Yy':
                # Update files based on user input
                main()

            # Return to main Menu
            print('\n >> Returning to main menu ...\n\n')
            
        elif usr_option == 2:
            print(strings.colors("\nICESat-2 not implemented yet !!!\n", 1))

        elif usr_option == 3:
            print(strings.colors("\nICESat-1 not implemented yet !!!\n", 1))

        elif usr_option == len(mainMenu_options):
            sys.exit('\n' + strings.colors('Goodbye, see you!', 1) + '\n')

