
def process_l1b_shot_deprecated(shot_index, beam, l1b_filename, l1b_h5, num_shots):

    # CHECK IF SHOTS IS WITHIN ROI PRIOR TO INSERT IT INTO MONGODB !!!

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

        print(f'Inserting Shot {shot_number} [{shot_index}/{num_shots}] of {beam}', end='')

        # Building dictionary to store shot data into MongoDB
        shot = {
            "uniqueID": '_'.join([d_str, orbit, track, beam_id, shot_number]),
            "beam": beam_id,
            "beam_type": l1b_h5[beam].attrs["description"],
            "date_acquire": d_iso,
            "orbit": orbit,
            "track": track,
            "shot_number": shot_number,
            "files_origin": [l1b_filename],
            "TanDEM_X_elevation": str(l1b_h5[beam + "/geolocation/digital_elevation_model"][shot_index]),
            "location": {
                "type": "Point",
                "coordinates": [
                    l1b_h5[beam + "/geolocation/longitude_bin0"][shot_index],
                    l1b_h5[beam + "/geolocation/latitude_bin0"][shot_index]
                    ]
            },
            "GEDI_L1B": {
                "rx_sample_count": str(sample_count),
                "rx_sample_start_index": str(startIndex),
                "rxwaveform": samples,
                "stale_return_flag": str(l1b_h5[beam + "/stale_return_flag"][shot_index]),
                "degrade": str(l1b_h5[beam + "/geolocation/degrade"][shot_index]),
                "elevation_bin0": str(l1b_h5[beam + "/geolocation/elevation_bin0"][shot_index]),
                "elevation_bin0_error": str(l1b_h5[beam + "/geolocation/elevation_bin0_error"][shot_index]),
                "elevation_lastbin": str(l1b_h5[beam + "/geolocation/elevation_lastbin"][shot_index]),
                "elevation_lastbin_error": str(l1b_h5[beam + "/geolocation/elevation_lastbin_error"][shot_index]),
                "latitude_bin0_error": str(l1b_h5[beam + "/geolocation/latitude_bin0_error"][shot_index]),
                "longitude_bin0_error": str(l1b_h5[beam + "/geolocation/longitude_bin0_error"][shot_index]),
                "surface_type": [
                    str(l1b_h5[beam + "/geolocation/surface_type"][i][shot_index]) for i in list(range(5))
                    ]
            }

        }

        # Storing data into MongoDB
        with pymongo.mongo_client.MongoClient() as mongo:
            db = mongo.get_database("gedi_test")
            db['shots'].insert_one(shot)
        
        print('... OK')
    else:
        print(
            'Shot {} [{}/{}]does not fall into ROI ... Moving on ...'.format(
                str(l1b_h5[beam + "/shot_number"][shot_index]),
                shot_index,
                num_shots
                )
            )
    
    
def process_l1b_beam_deprecated(l1b_h5, beam, l1b_filename):
    
    # Retrieve number of shots within a given GEDI Beam
    num_shots = len(l1b_h5[beam + "/shot_number"])

    # Retrieve and store shot data into MongoDB Database
    for shot_index in list(range(num_shots)):
        process_l1b_shot_deprecated(shot_index, beam, l1b_filename, l1b_h5, num_shots)