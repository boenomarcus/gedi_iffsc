import os, sys, h5py, pymongo, geojson
from datetime import datetime
from glob import glob
from utils import strings, numbers, db_storer, config


# Print greetings message
strings.greeting_gedi2mongo()

while True:

    # Options for main menu
    mainMenu_options = [
        'Update GEDI Shots Database',
        'Exit system'
        ]
    
    # Print options of actions for the user to select 
    print('> Main Menu:\n')
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
        usr_product = db_storer.select_product_to_update()
        
        # Ask for user confirmation
        answer = strings.yes_no_input(
            'Update MongoDB with new {} files?! [(y)/n] '.format(
                    strings.colors(usr_product, 2)
                    )
                )
            
        if answer in 'Yy':
            # Update files based on user input
            db_storer.update_db_files(usr_product)

        # Return to main Menu
        print('\n >> Returning to main menu ...\n\n')

    elif usr_option == len(mainMenu_options):
        sys.exit('\n' + strings.colors('Goodbye, see you!', 1) + '\n')

