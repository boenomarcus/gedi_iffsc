"""
OLMS Configuration

Set parameters to correct usage of OLMS utilities

Author: Marcus Moresco Boeno

"""

# Local storage for files downloaded from earthdata
localStorage = "C:\\Users\\marcu\\gedi_files"

# Name of base MongoDB database (without GEDI version)
base_mongodb = "gedi_sc"

# Bounding Box for the Santa Catarina State
default_bbox = [-25.9, -53.9, -29.5, -48]

# ROI for shot collection
roiPath = "C:\\Users\\marcu\\gedi_files\\GEO\\sc_b5k_s2k_edit.geojson"

# Available GEDI products and versions
gedi_products = ["GEDI01_B", "GEDI02_A", "GEDI02_B"]
gedi_versions = ["001"]
beam_list = [
    "BEAM0000", "BEAM0001", "BEAM0010", "BEAM0011", 
    "BEAM0101", "BEAM0110", "BEAM1000", "BEAM1011"
    ]

# Info for GEDI Extractor
basicInfo = [
    "shot_number", "degrade", "stale_return_flag", "l2a_quality_flag", 
    "l2b_quality_flag", "omega", "cover", "pai", "rh100", "fhd", "elev_TDX", 
    "elev_highest", "elev_ground", "beam", "date_acquired", "l1b_file", 
    "l2a_file", "l2b_file"
    ]

fullInfo = {
    "GEDI01_B": [
        "rx_sample_count", "rx_sample_start_index", "rxwaveform", "shot_number",
        "stale_return_flag", "gelocation/degrade", "geolocation/surface_type",  
        "geolocation/digital_elevation_model"
    ],
    "GEDI02_A": [
        "elev_highestreturn", "elev_lowestmode", "elevation_bias_flag",
        "num_detectedmodes", "sensitivity", "rx_assess/rx_assess_flag"
    ],
    "GEDI02_B": [
        "algorithmrun_flag", "cover", "cover_z", "fhd_normal", "omega", "pai", 
        "pai_z", "l2a_quality_flag", "l2b_quality_flag", "rg", "rh100", "rhog", 
        "rhov", "rhov_error", "rossg", "rv", "surface_flag"
    ]
}
