# Local storage for files downloaded from earthdata
localStorage = "C:\\Users\\marcu\\gedi_files"

# Name of base MongoDB database (without version)
#   gedi_sc_v001, gedi_sc_v002, ... gedi_sc_v00n 
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