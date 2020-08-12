import os, sys, h5py, pymongo, geojson
from datetime import datetime
from glob import glob
from shapely.geometry import Point, Polygon
from utils import strings, numbers, db_storer

# Local storage for files downloaded from earthdata and ROI
#   must have folders "GEDI01_B", "GEDI02_A", and "GEDI02_B" keeping gedi files
localStorage = 'C:\\Users\\marcu\\gedi_files'

# Name of MongoDB Datase
mongoDatabase = 'gedi_test_2'

# Available GEDI products and versions
gedi_products = ['GEDI01_B', 'GEDI02_A', 'GEDI02_B']
gedi_versions = ['001']


# Options for main menu
mainMenu_options = [
    'Update GEDI Shots Database',
    'Exit system'
    ]


def update_db_files(usr_option):

    # Path to folder keeping GEDI files downloaded from the EarthData Server
    path = localStorage + os.sep + usr_option + os.sep

    # Retrieve all paths to files in local storage
    files = glob(path + '*.h5')
    
    # Continue only if there are files into the folder
    if len(files) == 0:
        print('\n' + strings.colors(f'No {usr_option} files were found!', 1) + '\n')
    else:

        # Retrieving basenames
        files_baseName = [os.path.basename(f) for f in files]

        # Check if log of processed files exists into mongodb
        with pymongo.mongo_client.MongoClient() as mongo:
            db = mongo.get_database(mongoDatabase)
            if 'files_processed' in db.list_collection_names():
                
                # Retrieve processed files
                processed_files = db['files_processed'].find_one()[usr_option]
                
                # Get difference between all_filesPath and processd_files

                files_to_process

                files_to_process = list(
                    set(files_baseName) - set(processed_files)
                    )
            else:
                files_to_process = all_filePaths_bs
        
        # Call function to process files
        if usr_option == 'GEDI01_B':
            for gedi_file in files_to_process:
                print(f'\nProcessing file {gedi_file}...\n')
                db_storer.process_l1b_file(path + gedi_file)
                print(f'\nFile {gedi_file} successfully processed...\n')
        
        elif usr_option == 'GEDI02_A':
            for gedi_file in files_to_process:
                print(f'\nProcessing file {gedi_file}...\n')
                db_storer.process_l2a_file(path + gedi_file)
                print(f'\nFile {gedi_file} successfully processed...\n')

        elif usr_option == 'GEDI02_B':
            for gedi_file in files_to_process:
                print(f'\nProcessing file {gedi_file}...\n')
                db_storer.process_l2b_file(path + gedi_file)
                print(f'\nFile {gedi_file} successfully processed...\n')
    


        
                
    


def select_product_to_update():
    
    # Initializing message
    print('\n> Which product do you want to update?!\n')
    
    # Print options of products to update 
    for pos, options in enumerate(gedi_products):
        print(
            '[{}] {}'.format(
                strings.colors(pos+1, 3), strings.colors(options, 2)
            )
        )
    
    # Identifying next action
    usr_option = numbers.readOption(
        'Select an option: ', 
        len(gedi_products)
    )

    return gedi_products[usr_option-1]


def select_version_to_update():
    
    # Initializing message
    print('\n> Which version do you want to update?!\n')
    
    # Print options of products to update 
    for pos, options in enumerate(gedi_versions):
        print(
            '[{}] {}'.format(
                strings.colors(pos+1, 3), strings.colors(options, 2)
            )
        )
    
    # Identifying next action
    usr_option = numbers.readOption(
        'Select an option: ', 
        len(gedi_products)
    )

    return gedi_versions[usr_option-1]
    

    
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
                )
            )
    
        # Identifying next action
        usr_option = numbers.readOption(
            'Select an option: ', 
            len(mainMenu_options)
        ) 
        
        # Either Update MongoDB or Exit System
        if usr_option == 1:
            
            # Check which product and version are to update
            usr_product = select_product_to_update()
            usr_version = select_version_to_update()

            # Ask for user confirmation
            answer = strings.yes_no_input(
                'Update {} with new {} files?! [(y)/n] '.format(
                        strings.colors(mongoDatabase + 'v' + usr_version, 2),
                        strings.colors(usr_product, 2)
                    )
                )
            
            if answer in 'Yy':
                # Update files based on user input
                update_db_files(usr_product, usr_version)

            # Return to main Menu
            print('\n >> Returning to main menu ...\n\n')

        elif usr_option == len(mainMenu_options):
            sys.exit('\n' + strings.colors('Goodbye, see you!', 1) + '\n')
        
        
        
        
        
        

