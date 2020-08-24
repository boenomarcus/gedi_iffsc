import os
import sys
import urllib.request
import json
import h5py
import pymongo
import geojson
import pandas as pd
import numpy as np

from datetime import datetime
from shapely.geometry import Point, Polygon
from utils import strings


class GEDI_request(object):

    lpdaac_base_url = "https://lpdaacsvc.cr.usgs.gov/services/gedifinder?"

    def __init__(self, p, v, bbox):
        self.product = p
        self.version = v
        self.bbox = str(bbox).replace(" ","")
        self.output = "json"

    def process_request(self):

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
    Class to process gedi shots and store data into mongodb
    """

    def __init__(self, path, l1b, l2a, l2b, vers, strMatch, beams, db, extent):
        self.path = path
        self.l1b_file = l1b
        self.l2a_file = l2a
        self.l2b_file = l2b
        self.version = vers
        self.strMatch = strMatch
        self.beams = beams
        self.db = db
        self.extent = extent
    
    def update_process_log(self):
        """
        method to update log of processed files
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
        
        # Print message on file being processed
        print(f"\n> Processing files")
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
                "shot_number": l1b_h5[beam + "/shot_number"][l1b_beg:l1b_end],
                "degrade": l1b_h5[beam + "/geolocation/degrade"][l1b_beg:l1b_end],
                "stale_return_flag": l1b_h5[beam + "/stale_return_flag"][l1b_beg:l1b_end],
                "l2a_quality_flag": list(l2a_h5[beam + "/quality_flag"][l2a_beg:l2a_end]),
                "l2b_quality_flag": list(l2b_h5[beam + "/l2b_quality_flag"][l2b_beg:l2b_end])
            })
            
            # Get date of shots acquisition
            date_shots = datetime.strptime(self.l1b_file[21:26], "%y%j")
            
            # Crete empty list to store shots docs to insert into mongo
            shots = []

            # Iterating over the dataframe rows
            for index, row in df.iterrows():
                
                # Create Shapely Point to check if shot is within ROI area
                shot_geoLocation = Point(row["lat"], row["lon"])

                # Check if the shot is within ROI.
                # If True store data otherwise move to next shot
                if shot_geoLocation.within(self.extent) is True:
                
                    shots.append(
                        {
                            "location": {
                                "type": "Point",
                                "coordinates": [row["lon"], row["lat"]]
                            },
                            "shot_number": str(row["shot_number"]),
                            "degrade": str(row["degrade"]),
                            "stale_return_flag": str(row["stale_return_flag"]),
                            "l2a_quality_flag": str(row["l2a_quality_flag"]),
                            "l2b_quality_flag": str(row["l2b_quality_flag"]),
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