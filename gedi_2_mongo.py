import h5py, pymongo, os
from datetime import datetime


l1b_filepath = 'C:\\Users\\marcu\\Documents\\courses\\gedi_notebooks\\data\\processed_GEDI01_B_2019288081641_O04758_T01894_02_003_01.h5'
# l1b_filename = 'processed_GEDI01_B_2019288081641_O04758_T01894_02_003_01.h5'



def process_l1b_shot(shot_index, beam, l1b_filename, l1b_h5, num_shots):
    
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


def process_l1b_beam(l1b_h5, beam, l1b_filename):
    
    # Retrieve number of shots within a given GEDI Beam
    num_shots = len(l1b_h5[beam + "/shot_number"])

    # Retrieve and store shot data into MongoDB Database
    for shot_index in list(range(num_shots)):
        process_l1b_shot(shot_index, beam, l1b_filename, l1b_h5, num_shots)


def process_l1b_file(l1b_filepath):

    # Read HDF5 File
    l1b_h5 = h5py.File(l1b_filepath, 'r')  # 'r' <-- read-only mode

    # Find BEAMS nomenclature
    beam_list = [l for l in l1b_h5.keys() if l.startswith('BEAM')]

    # Process each L1B Beam
    for beam in beam_list:
        process_l1b_beam(l1b_h5, beam, os.path.basename(l1b_filepath))

if __name__ == '__main__':
    process_l1b_file(l1b_filepath)



# ---- Example of gedi shots

# {

#     # uniqueID = date_orbit_track_beam_shotNumber
#     "uniqueID": "20191015_O04758_T01894_0000_3121435",
#     "beam": "0000",
#     "beam_type": "Coverage Laser",
#     "date": "2019-10-15",
#     "orbit": "04758",
#     "track": "01894",
#     "files_origin": [
#         "processed_GEDI01_B_2019288081641_O04758_T01894_02_003_01.h5",
#         "processed_GEDI02_A_2019288081641_O04758_T01894_02_001_01.h5",
#         "processed_GEDI02_B_2019288081641_O04758_T01894_02_001_01.h5"
#     ],
#     "digital_elevetion_model": 200,
#     "GEDI_L1B":{

#         "rx_sample_count": 200,
#         "rx_sample_start_index": 20,
#         "rxwaveform": [1323.31, 123.43, ... , 1323.32, 123.31],
#         "stale_return_flag": 1,
#         "degrade": 1,
#         "elevation_bin0": 210,
#         "elevation_bin0_error": 1,
#         "elevation_lastbin": 280,
#         "elevation_lastbin_error": 1.2,
#         "latitude_bin0_error": 1.2,
#         "longitude_bin0_error": 4,
#         "surface_type": [1, 1, 0, 0, 0]

#     },
#     "GEDI_L2A":{

#         "elev_highestreturn": 1200,
#         "elev_lowestmode": 1120,
#         "elevation_bias_flag": 1,
#         "quality_flag": 1,
#         "rh": [rh0, rh1, rh2, ..., rh99, rh100, rh101],
#         "sensitivity": 95,
#         "quality_flag_rx_assess": 1,
#         "rx_assess_flag": 1

#     },
#     "GEDI_L2B":{
        
#         "algorithmrun_flag": 1,
#         "cover": 0.932,
#         "cover_z": [312, 123, ..., 0, 0],
#         "fhd_normal": 0.634,
#         "omega": 1,
#         "pai": 0.0234,
#         "pai_z": [0.0234, 0.635, ..., 0, 0],
#         "pavd_z": [0.0234, 0.635, ..., 0, 0],
#         "pgap_theta": 0.984,
#         "pgap_theta_error": 1.34,
#         "pgap_theta_z": 0.0880,
#         "l2b_quality_flag": 1,
#         "rg": 12809.25,
#         "rhog": 0.4,
#         "rhog_error": -9999,
#         "rhov": 0.6,
#         "rhov_error": -9999,
#         "rossg": 0.5,
#         "rv": 55.2853,
#         "rx_sample_count": 220,
#         "rx_sample_start_index": 200,
#         "stale_return_flag": 1,
#         "surface_flag": 1

#     },
#     "location": {type: "Point", "coordinates": [-48.9999, -26.5112]}

# }


# {

#     "uniqueID": "20191015_O04758_T01894_0000_3121435",
#     "beam": "0000",
#     "beam_type": "Coverage Laser",
#     "date": "2019-10-15",
#     "orbit": "04758",
#     "track": "01894",
#     "files_origin": [
#         "processed_GEDI01_B_2019288081641_O04758_T01894_02_003_01.h5",
#         "processed_GEDI02_A_2019288081641_O04758_T01894_02_001_01.h5",
#         "processed_GEDI02_B_2019288081641_O04758_T01894_02_001_01.h5"
#     ],
#     "digital_elevetion_model": 200,
#     "GEDI_L1B":{

#         "rx_sample_count": 200,
#         "rx_sample_start_index": 20,
#         "rxwaveform": [1323.31, 123.43, 1323.32, 123.31],
#         "stale_return_flag": 1,
#         "degrade": 1,
#         "elevation_bin0": 210,
#         "elevation_bin0_error": 1,
#         "elevation_lastbin": 280,
#         "elevation_lastbin_error": 1.2,
#         "latitude_bin0_error": 1.2,
#         "longitude_bin0_error": 4,
#         "surface_type": [1, 1, 0, 0, 0]

#     },
#     "GEDI_L2A":{

#         "elev_highestreturn": 1200,
#         "elev_lowestmode": 1120,
#         "elevation_bias_flag": 1,
#         "quality_flag": 1,
#         "rh": [0, 1, 2, 99, 100, 101],
#         "sensitivity": 95,
#         "quality_flag_rx_assess": 1,
#         "rx_assess_flag": 1

#     },
#     "GEDI_L2B":{
        
#         "algorithmrun_flag": 1,
#         "cover": 0.932,
#         "cover_z": [312, 123, 0, 0],
#         "fhd_normal": 0.634,
#         "omega": 1,
#         "pai": 0.0234,
#         "pai_z": [0.0234, 0.635, 0, 0],
#         "pavd_z": [0.0234, 0.635, 0, 0],
#         "pgap_theta": 0.984,
#         "pgap_theta_error": 1.34,
#         "pgap_theta_z": 0.0880,
#         "l2b_quality_flag": 1,
#         "rg": 12809.25,
#         "rhog": 0.4,
#         "rhog_error": -9999,
#         "rhov": 0.6,
#         "rhov_error": -9999,
#         "rossg": 0.5,
#         "rv": 55.2853,
#         "rx_sample_count": 220,
#         "rx_sample_start_index": 200,
#         "stale_return_flag": 1,
#         "surface_flag": 1

#     },
#     "location": {"type": "Point", "coordinates": [-48.9999, -26.5112]}

# }