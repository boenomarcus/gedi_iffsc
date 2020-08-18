import os, pymongo
from glob import glob
from utils import classes, strings, config, geoTasks

# Get ROI Shapely Polygon
roi_poly = geoTasks.shapelyPol_from_GeoJSONSinglePol(config.roiPath)


def update_gedi_db():
    """
    > update_gedi_db()
        Function to update gedi database on MongoDB.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs (function leads to MongoDB update).
    """

    # Get local storage files
    files = get_gedi_files()

    # Get gedi versions inside local storage
    versions = get_gedi_versions(files)

    # Get dictionary with files per version and product level
    files_dict = match_gedi_files(files, versions)

    # Get dictionary of files to process
    final_files, numgranules = gedi_files_to_Process(files_dict)

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
                    print(f"\nDownload all '{match}' Granules to continue!")
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
                f"\n > MongoDB '{config.base_mongodb}' succesfully updated!", 
                2
                )
            )


def get_gedi_files(path=config.localStorage):
    """
    > get_gedi_files(path=config.localStorage)
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
                      path + os.sep + product + os.sep + '*.h5'
                    )
                ]
            )
    
    # Return results
    return files


def get_gedi_versions(files):
    """
    > get_gedi_versions(files)
        Function to get versions of GEDI Granules in local storage.

    > Arguments:
        - files: list of GEDI filenames.
    
    > Output:
        - List of GEDI Versions in local storage.
    """
    return list(set([v[-5:-3] for v in files]))


def match_gedi_files(files, versions):
    """
    > get_gedi_versions(files, versions)
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


def gedi_files_to_Process(files_dict):
    """
    > get_gedi_versions(files, versions)
        Function to get dictionary of GEDI Granules to process.

    > Arguments:
        - files_dict: Dictionary of matching granules by version.
            --> Output from match_gedi_files().
    
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

