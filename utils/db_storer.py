import os, sys, h5py, pymongo, geojson
from glob import glob
from datetime import datetime
from shapely.geometry import Point, Polygon
from utils import strings, numbers, config, geoTasks

# Get ROI Shapely Polygon 
roi_poly = geoTasks.shapelyPol_from_GeoJSONSinglePol(config.roiPath)

def update_db_files(usr_product):
    """
    > update_db_files(usr_product)
        Function to update gedi shots collection on MongoDB Based on user input

    > Arguments:
        - usr_product: GEDI Product Level (GEDI01_B, GEDI02_A or GEDI02_B).
    
    > Output:
        - No outputs (function leads to MongoDB update).
    """
    
    # Path to folder keeping GEDI files downloaded from the EarthData Server
    path = config.localStorage + os.sep + usr_product + os.sep

    # Retrieve all files in local storage
    files = [os.path.basename(f) for f in glob(path + '*.h5')]
        
    # Continue only if there are files into the folder
    if len(files) == 0:
        print(strings.colors(f'\nNo {usr_product} files were found!\n', 1))

    else:

        print(f'\nFiles on local storage: {len(files)}')

        # Empty list to store files to process
        files_to_process = []

        # Retrieve granule versions in the folder
        gedi_versions = list(set([v[-5:-3] for v in files]))

        for version in gedi_versions:

            # Files
            files_v = [f for f in files if f.endswith(version + '.h5')]

            # Check if log of processed files exists into mongodb
            with pymongo.mongo_client.MongoClient() as mongo:
                
                # Get DB
                db = mongo.get_database(config.base_mongodb + '_v' + version)
                
                # Get list of files to process

                # Test whether log exists or not
                if 'files_processed' in db.list_collection_names():
                    
                    # Retrieve doc cointaining processed files
                    processed_files = db['files_processed'].find_one()
                    
                    if usr_product in list(processed_files.keys()):
                                            
                        # Get lits of unprocessed files
                        files_to_process.extend(
                            list(set(files_v) - set(processed_files[usr_product]))
                        )
                    else:
                        # Case when L1B exists, but not L2A and L2B
                        # Get lits of unprocessed files
                        files_to_process.extend(files_v)
                else:

                    # If there are no log, process all files
                    files_to_process.extend(files_v)
        
        print(
            'Files already processed: {}'.format(
                len(files) - len(files_to_process)
                )
            )
        print(
            'Files to process: {}'.format(
                len(files_to_process)
                )
            )

        # Function calls to process files and update log
        for gedi_file in files_to_process:

            # Processing file
            print(
                strings.colors(
                    f'\nProcessing file {gedi_file}...\n',
                    2
                    )
                )
            process_gedi_file(path + gedi_file, usr_product)
            print(
                strings.colors(
                    f'\nFile {gedi_file} successfully processed...\n',
                    2
                    )
                )

            # Update process log
            with pymongo.mongo_client.MongoClient() as mongo:
                
                # Get DB
                db_name = config.base_mongodb + '_v' + gedi_file[-5:-3]
                db = mongo.get_database(db_name)
                
                # Check if DB exists
                db_list = mongo.list_database_names()

                if db_name in db_list:
                    # Update document      
                    db['files_processed'].find_one_and_update(
                        {}, # empty to update the first doc in the collection
                        {"$push": {usr_product: gedi_file}}
                        )
                else:
                    # Create document
                    db['files_processed'].insert_one({usr_product: [gedi_file]})
                
                
def select_product_to_update():
    """
    > select_product_to_update()
        Function to select which GEDI Product Level to update

    > Arguments:
        - No arguments.
    
    > Output:
        - str: GEDI Product Level (GEDI01_B, GEDI02_A or GEDI02_B).
    """
    # Initializing message
    print('\n> Which product do you want to update?!\n')
    
    # Print options of products to update 
    for pos, options in enumerate(config.gedi_products):
        print(
            '[{}] {}'.format(
                strings.colors(pos+1, 3), strings.colors(options, 2)
            )
        )
    
    # Identifying user option
    usr_option = numbers.readOption(
        'Select an option: ', 
        len(config.gedi_products)
    )

    return config.gedi_products[usr_option-1]


def select_version_to_update():
    """
    >>>> DEPRECATED <<<<<

    > select_version_to_update()
        Function to select which GEDI Product Version to update

    > Arguments:
        - No arguments.
    
    > Output:
        - str: GEDI Product Version (001, 002, ...).
    """
    # Initializing message
    print('\n> Which version do you want to update?!\n')
    
    # Print options of products to update 
    for pos, options in enumerate(config.gedi_versions):
        print(
            '[{}] {}'.format(
                strings.colors(pos+1, 3), strings.colors(options, 2)
            )
        )
    
    # Identifying user option
    usr_option = numbers.readOption(
        'Select an option: ', 
        len(config.gedi_versions)
    )

    return config.gedi_versions[usr_option-1]

def process_l2a_l2b_beam(l2a_h5, num_beams, beam_i, beam, l2a_filename, usr_product):
    """
    > process_l2a_beam(l2a_h5, beam, l2a_filename)
        Function to process GEDI02_A BEAMs and update shot data.
        BEAMs are batch processed, meaning that up to a 1000 GEDI shots
        are updated into MongoDB per round. If a certain BEAMS has 2500 shots, 
        three rounds will be performed.

    > Arguments:
        - l2a_h5: h5py.File() connection to GEDI02_A HDF5 file;
        - num_beams: Number of GEDI BEAMs;
        - beam_i: BEAM index;
        - beam: GEDI BEAM (BEAM0000, BEAM0001, ...);
        - l2a_filename: GEDI02_A HDF5 file basename;
        - usr_product: GEDI Product Level (GEDI01_B, GEDI02_A or GEDI02_B).
    
    > Output:
        - No outputs (function leads to MongoDB update).
    """
    print(f'  > {beam} [{beam_i+1}/{num_beams}]')
    # Retrieve number of shots within a given GEDI Beam
    num_shots = len(l2a_h5[beam + "/shot_number"])

    # Define number of rounds
    if num_shots // 1000 < 1:
        num_rounds = 1
    elif num_shots % 1000 > 0:
        num_rounds = num_shots // 1000 + 1
    else:
        num_rounds = num_shots // 1000

    # Creating begin and end indexes for the batch rounds
    indexes = [[i*1000, i*1000 + 999] for i in list(range(num_rounds))]

    # Adjusting last index to number of shots in the beam
    if indexes[-1][1] >= num_shots:
        indexes[-1][1] = num_shots - 1
    
    # GEDI Version
    version = l2a_filename[-5:-3]

    # Process batches of GEDI Shots
    for begin_index, end_index in indexes:
        
        print(
            '   > Updating shots {} to {} [{} total] ...'.format(
                begin_index, end_index, num_shots-1
                )
            )
        
        # Update shot data
        process_l2a_l2b_shots(
            begin_index, end_index, beam, l2a_filename, l2a_h5, usr_product
            )


def process_l2a_l2b_shots(begin_index, end_index, beam, l2_filename, l2_h5, usr_product):
    """
    > process_l2a_shots(begin_index, end_index, beam, l1b_filename, l1b_h5)
        Function to process GEDI01_B BEAMs and update shot data.
        BEAMs are batch processed, meaning that up to a 1000 GEDI shots
        are stored into MongoDB per round. If a certain BEAMS has 2500 shots, 
        three rounds will be performed.

    > Arguments:
        - begin_index: Index of the first shot to collect;
        - end_index: Index of the last shot to collect;
        - beam: GEDI BEAM (BEAM0000, BEAM0001, ...);
        - l2_filename: GEDI02 HDF5 file basename;
        - l2_h5: h5py.File() connection to GEDI02 HDF5 file;
        - usr_product: GEDI Product Level (GEDI01_B, GEDI02_A or GEDI02_B).
    
    > Output:
        - List of dictionaries containing shot data.
    """
    
    with pymongo.mongo_client.MongoClient() as mongo:
                
        # Get DB
        db = mongo.get_database(
            config.base_mongodb + '_v' + l2_filename[-5:-3]
            )

        # Initialize bulk operation
        bulk = db['shots'].initialize_ordered_bulk_op()
            
        # Iterate upon shot indexes
        for shot_index in list(range(begin_index, end_index + 1)):

            # Build uniqueID field to create query
            # 
            # Info on date of acquisition
            d_iso = datetime.strptime(l2_filename[21:26], '%y%j')
            d_str = str(d_iso.year) + str(d_iso.month).zfill(2) + str(d_iso.day).zfill(2) 
            #
            # Info on orbit, track, beam id and shot number
            orbit = l2_filename[33:39]
            track = l2_filename[40:46]
            shot_number = str(l2_h5[beam + "/shot_number"][shot_index])
            beam_id = bin(l2_h5[beam + "/beam"][0])[2:].zfill(4)

            if usr_product == 'GEDI02_A':
                # Create query
                bulk.find(
                    {"uniqueID": '_'.join([d_str, orbit, track, beam_id, shot_number])}
                ).update(
                    {'$set': {
                        "l2a": True,
                        "l2a_quality_flag": str(l2_h5[beam + "/quality_flag"][shot_index]),
                        "sensitivity": str(l2_h5[beam + "/sensitivity"][shot_index]),
                        "elev_highestreturn": str(l2_h5[beam + "/elev_highestreturn"][shot_index])
                        },
                    "$push": {
                        "files_origin": [l2_filename]
                    }
                        }
                    )
            else:
                    # Create query
                bulk.find(
                    {"uniqueID": '_'.join([d_str, orbit, track, beam_id, shot_number])}
                ).update(
                    {'$set': {
                        "l2b": True,
                        "l2b_quality_flag": str(l2_h5[beam + "/l2b_quality_flag"][shot_index])
                        },
                    "$push": {
                        "files_origin": [l2_filename]
                    }
                        }
                    )
        
        # Execute bulk update
        bulk.execute()


def process_l1b_beam(l1b_h5, num_beams, beam_i, beam, l1b_filename):
    """
    > process_l1b_beam(l1b_h5, beam, l1b_filename)
        Function to process GEDI01_B BEAMs and update shot data.
        BEAMs are batch processed, meaning that up to a 1000 GEDI shots
        are stored into MongoDB per round. If a certain BEAMS has 2500 shots, 
        three rounds will be performed.

    > Arguments:
        - l1b_h5: h5py.File() connection to GEDI01_B HDF5 file;
        - num_beams: Number of GEDI BEAMs;
        - beam_i: BEAM index;
        - beam: GEDI BEAM (BEAM0000, BEAM0001, ...);
        - l1b_filename: GEDI01_B HDF5 file basename.
    
    > Output:
        - No outputs (function leads to MongoDB update).
    """
    print(f'  > {beam} [{beam_i+1}/{num_beams}]')
    # Retrieve number of shots within a given GEDI Beam
    num_shots = len(l1b_h5[beam + "/shot_number"])

    # Define number of rounds
    if num_shots // 1000 < 1:
        num_rounds = 1
    elif num_shots % 1000 > 0:
        num_rounds = num_shots // 1000 + 1
    else:
        num_rounds = num_shots // 1000

    # Creating begin and end indexes for the batch rounds
    indexes = [[i*1000, i*1000 + 999] for i in list(range(num_rounds))]

    # Adjusting last index to number of shots in the beam
    if indexes[-1][1] >= num_shots:
        indexes[-1][1] = num_shots - 1
    
    # GEDI Version
    version = l1b_filename[-5:-3]

    # Process batches of GEDI Shots
    for begin_index, end_index in indexes:
        
        print(
            '    > Storing shots {} to {} [{} total] ...'.format(
                begin_index, end_index, num_shots-1
                )
            )
        
        # Retrieve shot data
        shots = process_l1b_shots(
            begin_index, end_index, beam, l1b_filename, l1b_h5
            )

        # Store data into MongoDB
        if len(shots) > 0:
            with pymongo.mongo_client.MongoClient() as mongo:
                
                # Get DB
                db = mongo.get_database(
                    config.base_mongodb + '_v' + version
                    )
                
                # Upload up to 1000 GEDI Shots into MongoDB Shot Collection
                db['shots'].insert_many(shots)


def process_l1b_shots(begin_index, end_index, beam, l1b_filename, l1b_h5):
    """
    > process_l1b_shots(begin_index, end_index, beam, l1b_filename, l1b_h5)
        Function to process GEDI01_B BEAMs and update shot data.
        BEAMs are batch processed, meaning that up to a 1000 GEDI shots
        are stored into MongoDB per round. If a certain BEAMS has 2500 shots, 
        three rounds will be performed.

    > Arguments:
        - begin_index: Index of the first shot to collect;
        - end_index: Index of the last shot to collect;
        - beam: GEDI BEAM (BEAM0000, BEAM0001, ...);
        - l1b_filename: GEDI01_B HDF5 file basename;
        - l1b_h5: h5py.File() connection to GEDI01_B HDF5 file.
    
    > Output:
        - List of dictionaries containing shot data.
    """

    # Create empty list to store data
    doc_list = []

    # Iterate upon shot indexes
    for shot_index in list(range(begin_index, end_index + 1)):

        # Create Shapely Point to check if shot is within ROI area
        shot_geoLocation = Point(
            l1b_h5[beam + "/geolocation/latitude_bin0"][shot_index],
            l1b_h5[beam + "/geolocation/longitude_bin0"][shot_index]
            )

        # Check if the shot is within ROI.
        # If True store data otherwise move to next shot
        if shot_geoLocation.within(roi_poly) is True:

            # Extracting info to populate shot dictionary to mongodb
            # 
            # Info on date of acquisition
            d_iso = datetime.strptime(l1b_filename[21:26], '%y%j')
            d_str = str(d_iso.year) + str(d_iso.month).zfill(2) + str(d_iso.day).zfill(2) 
            #
            # Info on orbit, track, beam id and shot number
            orbit = l1b_filename[33:39]
            track = l1b_filename[40:46]
            shot_number = str(l1b_h5[beam + "/shot_number"][shot_index])
            beam_id = bin(l1b_h5[beam + "/beam"][0])[2:].zfill(4)
            #
            # Info on waveform measurements
            #startIndex = l1b_h5[beam + "/rx_sample_start_index"][shot_index]
            #sample_count = l1b_h5[beam + "/rx_sample_count"][shot_index]
            #samples = list(l1b_h5[beam + "/rxwaveform"][int(startIndex-1):int(startIndex-1)+sample_count])
            #samples = [str(n) for n in samples]

            # Building dictionary to store shot data into MongoDB
            shot = {
                "uniqueID": '_'.join([d_str, orbit, track, beam_id, shot_number]),
                "beam": beam_id,
                "beam_type": l1b_h5[beam].attrs["description"],
                "date_acquire": d_iso,
                "shot_number": shot_number,
                "stale_return_flag": str(l1b_h5[beam + "/stale_return_flag"][shot_index]),
                "degrade": str(l1b_h5[beam + "/geolocation/degrade"][shot_index]),
                "files_origin": [l1b_filename],
                "TanDEM_X_elevation": str(l1b_h5[beam + "/geolocation/digital_elevation_model"][shot_index]),
                "l2a": False,
                "l2b": False,
                "location": {
                    "type": "Point",
                    "coordinates": [
                        l1b_h5[beam + "/geolocation/longitude_bin0"][shot_index],
                        l1b_h5[beam + "/geolocation/latitude_bin0"][shot_index]
                        ]
                }
            }

            # Appending found shot to list
            doc_list.append(shot)
    
    # Return results
    return doc_list


def process_gedi_file(filepath, gedi_level):
    """
    > process_gedi_file(filepath)
        Function to process GEDI files and update shot data

    > Arguments:
        - filepath: Full path to GEDI file;
        - gedi_level: GEDI Product Level (GEDI01_B, GEDI02_A or GEDI02_B).
    
    > Output:
        - No outputs (function leads to MongoDB update).
    """

    # Create connection to HDF5 File
    gedi_h5 = h5py.File(filepath, 'r')  # 'r' <-- read-only mode

    # Retrieve GEDI BEAMs nomenclature
    beam_list = [l for l in gedi_h5.keys() if l.startswith('BEAM')]

    # Process GEDI BEAMs
    if gedi_level == 'GEDI01_B':
        for beam_i, beam in enumerate(beam_list):
            process_l1b_beam(
                gedi_h5, 
                len(beam_list), 
                beam_i, 
                beam, 
                os.path.basename(filepath)
                )
    
    else:
        for beam_i, beam in enumerate(beam_list):
            process_l2a_l2b_beam(
                gedi_h5, 
                len(beam_list), 
                beam_i, 
                beam, 
                os.path.basename(filepath), 
                gedi_level
                )

