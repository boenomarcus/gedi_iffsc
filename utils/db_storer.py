
def process_l1b_beam(l1b_h5, beam, l1b_filename):

    # Retrieve number of shots within a given GEDI Beam
    num_shots = len(l1b_h5[beam + "/shot_number"])

    # Define number of rounds
    if num_shots // 1000 < 1:
        num_rounds = 1
    elif num_shots % 1000 > 0:
        num_rounds = num_shots // 1000 + 1
    else:
        num_rounds = num_shots // 1000

    # Creating begin and end indexes for the batch runs
    indexes = [[i*1000, i*1000 + 999] for i in list(range(num_rounds))]

    # Adjusting last index to number of shots in the beam
    if indexes[-1][1] >= num_shots:
        indexes[-1][1] = num_shots - 1

    # Process batches of GEDI Shots
    for begin_index, end_index in indexes:
        
        print(
            '\n\nStoring shots {} to {} [{} total] from {} [file = {}]'.format(
                begin_index, end_index, num_shots, beam, l1b_filename
                )
            )
        
        # Retrieve shot data
        print('> Retrieving and structuring data ...')
        shots = process_l1b_shots(
            begin_index, end_index, beam, l1b_filename, l1b_h5
            )

        # Store data into MongoDB
        print('> Storing data into MongoDB ...', end = '')
        if len(shots) > 0:
            with pymongo.mongo_client.MongoClient() as mongo:
                db = mongo.get_database("gedi_test_4")
                db['shots'].insert_many(shots)
        

def process_l1b_shots(begin_index, end_index, beam, l1b_filename, l1b_h5):
    
    doc_list = []

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
            startIndex = l1b_h5[beam + "/rx_sample_start_index"][shot_index]
            sample_count = l1b_h5[beam + "/rx_sample_count"][shot_index]
            samples = list(l1b_h5[beam + "/rxwaveform"][int(startIndex-1):int(startIndex-1)+sample_count])
            samples = [str(n) for n in samples]

            # Building dictionary to store shot data into MongoDB
            shot = {
                "uniqueID": '_'.join([d_str, orbit, track, beam_id, shot_number]),
                "beam": beam_id,
                "beam_type": l1b_h5[beam].attrs["description"],
                "date_acquire": d_iso,
                "shot_number": shot_number,
                "files_origin": [l1b_filename],
                "TanDEM_X_elevation": str(l1b_h5[beam + "/geolocation/digital_elevation_model"][shot_index]),
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
        



def shapelyPol_from_GeoJSONSinglePol(geo_filepath):
    
    # Open GeoJSON file
    with open(geo_filepath) as f:
        gj = geojson.load(f)

    # Subset info of interest
    coords = gj['features'][0]['geometry']['coordinates'][0]

    # Return shapely polygon
    return Polygon([tuple([pair[1], pair[0]]) for pair in coords])


def process_l1b_file(l1b_filepath):

    # Read HDF5 File
    l1b_h5 = h5py.File(l1b_filepath, 'r')  # 'r' <-- read-only mode

    # Find BEAMS nomenclature
    beam_list = [l for l in l1b_h5.keys() if l.startswith('BEAM')]

    # Process each L1B Beam
    for beam in beam_list:
        process_l1b_beam(l1b_h5, beam, os.path.basename(l1b_filepath))