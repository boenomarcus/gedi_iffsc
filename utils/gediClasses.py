"""
GEDI Classes

Classes to make HTTPS requests and store GEDI Shot data on MongoDB

Author: Marcus Moresco Boeno

"""

# Standard library imports
import os
import sys
import urllib.request
import json

# library specific imports
from datetime import datetime
from shapely.geometry import Point

# Third party library imports
import h5py
import pymongo
import pandas as pd
import numpy as np

# Local application imports
from utils import strings


class GEDI_request(object):
    """
    GEDI_request class

    Make HTTPS requests to LPDAAC_NASA Server to retrieve GEDI Granules
    of interest

    Attributes:
        - lpdaac_base_url: LPDAAC_NASA GEDI Finder base URL;
        - product: GEDI Product Level;
        - version: GEDI Product Version;
        - bbox: Bounding of of ROI (see config.default_bbox);
        - output: Output format (always set to "json").
    
    Methods:
        - process_request(self): Process request and return list of
            GEDI granules of interest.

    """
    # GEDI Finder base URL
    lpdaac_base_url = "https://lpdaacsvc.cr.usgs.gov/services/gedifinder?"

    def __init__(self, p, v, bbox):
        self.product = p
        self.version = v
        self.bbox = str(bbox).replace(" ","")
        self.output = "json"

    def process_request(self):
        """
        > process_request(self)
            Make GEDI Finder Request and return list of GEDI Granules.

        > Arguments:
            - self: GEDI_request instance.
        
        > Output:
            - list: List of GEDI Granules matching a given Bouding Box.
        """
        # Crete URL to access LP DAAC GEDI-Finder
        url = self.lpdaac_base_url + "product=" + self.product
        url += "&version=" + self.version
        url += "&bbox=" + self.bbox
        url += "&output=" + self.output
        
        # Read data as a JSON file
        with urllib.request.urlopen(url) as webPage:
            data = json.loads(webPage.read().decode())
        
        # Return list of HTTPS links for download steps 
        return data["data"]


class GEDI_Shots():
    """
    GEDI_Shots class

    Store GEDI Shots data into MongoDB Database

    Attributes:
        - path: Full path to local storage (see config.localStorage)
        - l1b_file: GEDI01_B filename that contains the given GEDI Shot
        - l2a_file: GEDI02_A filename that contains the given GEDI Shot
        - l2b_file: GEDI02_B filename that contains the given GEDI Shot
        - version: GEDI Product Version
        - strMatch: String with Granule unique ID
        - beams: GEDI BEAM List [BEAM0000, BEAM0001, ..., BEAM1011]
        - db: Default database (see config.base_mongodb)
        - extent: Limiting extent to db inserts (see config.roiPath)
        - index_gran: Batch index for granule
        - num_grans: Number of granule being batch processed
    
    Methods:
        - update_process_log(self): Update log of files processed
        - process_and_store(self): Insert Shot data into MongoDB

    """
    def __init__(self, path, l1b, l2a, l2b, vers, strMatch, beams, db, extent, index_gran, num_grans):
        self.path = path
        self.l1b_file = l1b
        self.l2a_file = l2a
        self.l2b_file = l2b
        self.version = vers
        self.strMatch = strMatch
        self.beams = beams
        self.db = db
        self.extent = extent
        self.index_gran = index_gran
        self.num_grans = num_grans
    
    def update_process_log(self):
        """
        > process_request(self)
            Update log of files processed into MongoDB Database.

        > Arguments:
            - self: GEDI_Shots instance.
        
        > Output:
            - No outputs (leads to process log update).
        """
        with pymongo.mongo_client.MongoClient() as mongo:
                    
            # Get DB
            db = mongo.get_database(self.db)
                    
            # Upload log
            db["processed_v" + self.version].insert_one({
                "str2match": self.strMatch,
                "l1b": self.l1b_file,
                "l2a": self.l2a_file,
                "l2b": self.l2b_file
            })
    
    def process_and_store(self):
        """
        > process_request(self)
            Insert Shot data into MongoDB.

        > Arguments:
            - self: GEDI_Shots instance.
        
        > Output:
            - No outputs (leads to Shot data insertion).
        """        
        # Print message on file being processed
        print(f"\n> Processing files ({self.index_gran}/{self.num_grans})")
        print(strings.colors(f"     > {self.l1b_file}", 3))
        print(strings.colors(f"     > {self.l1b_file}", 3))
        print(strings.colors(f"     > {self.l1b_file}", 3))


        # Iterate over BEAM list
        for beam in self.beams:

            # Print info on beam being processed
            print(f"          > {beam}")

            # Create connection with HDF5 files
            l1b_h5 = h5py.File(
                self.path + os.sep + "GEDI01_B" + os.sep + self.l1b_file, 
                "r"
                )
            l2a_h5 = h5py.File(
                self.path + os.sep + "GEDI02_A" + os.sep + self.l2a_file, 
                "r"
            )
            l2b_h5 = h5py.File(
                self.path + os.sep + "GEDI02_B" + os.sep + self.l2b_file, 
                "r"
                )


            # Getting indexes for matching shots on L1B, L2A and L2B products
            # Check L1B shot_numbers
            firstShots = [
                l1b_h5[beam + "/shot_number"][0],
                l2a_h5[beam + "/shot_number"][0],
                l2b_h5[beam + "/shot_number"][0]
            ]

            lastShots = [
                l1b_h5[beam + "/shot_number"][-1],
                l2a_h5[beam + "/shot_number"][-1],
                l2b_h5[beam + "/shot_number"][-1]
            ]

            # Get shots that are common to all products
            firstShot, lastShot = max(firstShots), min(lastShots)
            
            # L1B Begin and End indexes
            l1b_firstShot = np.where(l1b_h5[beam + "/shot_number"][:] == firstShot)
            l1b_beg = [l for l in l1b_firstShot][0].flat[0]
            l1b_lastShot = np.where(l1b_h5[beam + "/shot_number"][:] == lastShot)
            l1b_end = [l for l in l1b_lastShot][0].flat[0]
            
            # L2A Begin and End indexes
            l2a_firstShot = np.where(l2a_h5[beam + "/shot_number"][:] == firstShot)
            l2a_beg = [l for l in l2a_firstShot][0].flat[0]
            l2a_lastShot = np.where(l2a_h5[beam + "/shot_number"][:] == lastShot)
            l2a_end = [l for l in l2a_lastShot][0].flat[0]

            # L2A Begin and End indexes
            l2b_firstShot = np.where(l2b_h5[beam + "/shot_number"][:] == firstShot)
            l2b_beg = [l for l in l2b_firstShot][0].flat[0]
            l2b_lastShot = np.where(l2b_h5[beam + "/shot_number"][:] == lastShot)
            l2b_end = [l for l in l2b_lastShot][0].flat[0]
            
            # Create pandas dataframe to store shots data
            df = pd.DataFrame({
                "lat": l1b_h5[beam + "/geolocation/latitude_bin0"][l1b_beg:l1b_end],
                "lon": l1b_h5[beam + "/geolocation/longitude_bin0"][l1b_beg:l1b_end],
                "shot_number": [str(s) for s in l1b_h5[beam + "/shot_number"][l1b_beg:l1b_end]],
                "degrade": l1b_h5[beam + "/geolocation/degrade"][l1b_beg:l1b_end],
                "stale_return_flag": l1b_h5[beam + "/stale_return_flag"][l1b_beg:l1b_end],
                "l2a_quality_flag": l2a_h5[beam + "/quality_flag"][l2a_beg:l2a_end],
                "l2b_quality_flag": l2b_h5[beam + "/l2b_quality_flag"][l2b_beg:l2b_end],
                "omega": l2b_h5[beam + "/omega"][l2b_beg:l2b_end],
                "cover": l2b_h5[beam + "/cover"][l2b_beg:l2b_end],
                "pai": l2b_h5[beam + "/pai"][l2b_beg:l2b_end],
                "rh100": l2b_h5[beam + "/rh100"][l2b_beg:l2b_end],
                "fhd": l2b_h5[beam + "/fhd_normal"][l2b_beg:l2b_end],
                "elev_TDX": l1b_h5[beam + "/geolocation/digital_elevation_model"][l1b_beg:l1b_end],
                "elev_highest": l2a_h5[beam + "/elev_highestreturn"][l2a_beg:l2a_end],
                "elev_ground": l2a_h5[beam + "/elev_lowestmode"][l2a_beg:l2a_end]
            })
            
            # Get date of shots acquisition
            date_shots = datetime.strptime(self.l1b_file[21:26], "%y%j")
            
            # Crete empty list to store shots docs to insert into mongo
            shots = []

            # Iterating over the dataframe rows
            for index, row in df.iterrows():
                
                # Create Shapely Point to check if shot is within ROI area
                shot_geoLocation = Point(row["lon"], row["lat"])

                # Check if the shot is within ROI.
                # If True store data otherwise move to next shot
                if shot_geoLocation.within(self.extent) is True:
                
                    shots.append(
                        {
                            "location": {
                                "type": "Point",
                                "coordinates": [row["lon"], row["lat"]]
                            },
                            "shot_number": row["shot_number"],
                            "degrade": str(row["degrade"]),
                            "stale_return_flag": str(row["stale_return_flag"]),
                            "l2a_quality_flag": str(row["l2a_quality_flag"]),
                            "l2b_quality_flag": str(row["l2b_quality_flag"]),
                            "omega": str(row["omega"]),
                            "cover": str(row["cover"]),
                            "pai": str(row["pai"]),
                            "rh100": str(row["rh100"]),
                            "fhd": str(row["fhd"]),
                            "elev_TDX": str(row["elev_TDX"]),
                            "elev_highest": str(row["elev_highest"]),
                            "elev_ground": str(row["elev_ground"]),
                            "beam": beam,
                            "date_acquired": date_shots,
                            "l1b_file": self.l1b_file,
                            "l2a_file": self.l2a_file,
                            "l2b_file": self.l2b_file
                        }
                    )
            
            # Store data into MongoDB
            if len(shots) > 0:
                with pymongo.mongo_client.MongoClient() as mongo:
                    
                    # Get DB
                    db = mongo.get_database(self.db)
                    
                    # Upload up to 1000 GEDI Shots into MongoDB Shot Collection
                    db["shots_v" + self.version].insert_many(shots)

