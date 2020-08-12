import os, sys, h5py, pymongo, geojson
from datetime import datetime
from glob import glob
from shapely.geometry import Point, Polygon
from utils import strings, numbers, db_storer

# Local storage for files downloaded from earthdata and ROI
#   must have folders "GEDI01_B", "GEDI02_A", and "GEDI02_B" keeping gedi files
localStorage = 'C:\\Users\\marcu\\gedi_files'

# Available GEDI products and versions
gedi_products_list = ['GEDI01_B', 'GEDI02_A', 'GEDI02_B']
gedi_versions_list = ['001']

# Options for main menu
mainMenu_options = [
        'Update MongoDB GEDI Shots Collection',
        'Exit system'
    ]

l1b_filepath = 'C:\\Users\\marcu\\Documents\\iffsc\\gedi_data_ex\\processed_GEDI01_B_2019300171956_O04950_T04703_02_003_01.h5'
# l1b_filename = 'processed_GEDI01_B_2019288081641_O04758_T01894_02_003_01.h5'



def update_db_files(usr_option):
    
    # Path to folder keeping GEDI files downloaded from the EarthData Server
    path = localStorage + os.sep + usr_option + os.sep

    # Retrieve all paths to files in local storage
    all_filePaths = glob(path + '*.h5')

    if len(all_filePaths) == 0:
        print('\n' + strings.colors(f'No {usr_option} files were found!', 1) + '\n')
    else:

        # Check if log of processed files exists into mongodb

        if not exists:
            ###
        
        # Check if any of the files has already been processed
        with pymongo.mongo_client.MongoClient() as mongo:
            db = mongo.get_database('gedi_test_2')
            processd_files = db['files_processed'].find_one()[usr_option]        
    


def select_product_to_update():
    
    # Initializing message
    print('\n> Which product do you want to update?!\n')
    
    # Print options of products to update 
    for pos, options in enumerate(gedi_products_list):
        print(
            '[{}] {}'.format(
                strings.colors(pos+1, 3), strings.colors(options, 2)
            ),'\n'
        )
    
    # Identifying next action
    usr_option = numbers.readOption(
        'Select an option: ', 
        len(gedi_products_list)
    )

    # Call function to update MongoDB documents
    update_db_files(gedi_products_list[usr_option-1])

    # Return to main Menu
    print('\n >> Returning to main menu ...\n\n')


if __name__ == '__main__':

    # Print greetings message
    strings.greeting_gedi2mongo()

    while True:
        # Initializing message
        print('> Main Menu:\n')

        # Print options of actions for the user to select 
        for pos, options in enumerate(mainMenu_options):
            print(
                '[{}] {}'.format(
                    strings.colors(pos+1, 3), strings.colors(options, 2)
                ),'\n'
            )
    
        # Identifying next action
        usr_option = numbers.readOption(
            'Select an option: ', 
            len(mainMenu_options)
        ) 
        
        # Either Update MongoDB or Exit System
        if usr_option == 1:
            select_product_to_update()
        elif usr_option == 2:
            sys.exit('\n' + strings.colors('Goodbye, see you!', 1) + '\n')

