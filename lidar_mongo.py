import sys
from utils import strings, numbers, gediTasks

# Print greetings message
strings.greeting_lidar_mongo()

while True:
    # Options for main menu
    mainMenu_options = [
        'Update GEDI Shots Database',
        'Update ICESat-2 (ATLAS) Database',
        'Update ICESat (GLAS) Database',
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
                
        # Ask for user confirmation
        answer = strings.yes_no_input(
            '\nUpdate MongoDB with new GEDI files?! [(y)/n] '
            )
                
        if answer in 'Yy':
            # Update GEDI Database
            gediTasks.update_gedi_db()

        # Return to main Menu
        print('\n >> Returning to main menu ...\n\n')
            
    elif usr_option == 2:
        print(strings.colors("\nICESat-2 not implemented yet !!!\n", 1))

        # # Ask for user confirmation
        # answer = strings.yes_no_input(
        #     '\nUpdate MongoDB with new ICESat-2 files?! [(y)/n] '
        #     )
                
        # if answer in 'Yy':
        #     # Update ICESat-2 Database
        #     ic2Tasks.update_ic2_db()

        # # Return to main Menu
        # print('\n >> Returning to main menu ...\n\n')

    elif usr_option == 3:
        print(strings.colors("\nICESat-1 not implemented yet !!!\n", 1))

        # # Ask for user confirmation
        # answer = strings.yes_no_input(
        #     '\nUpdate MongoDB with new ICESat-1 files?! [(y)/n] '
        #     )
                
        # if answer in 'Yy':
        #     # Update ICESat-1 Database
        #     ic1Tasks.update_ic1_db()

        # # Return to main Menu
        # print('\n >> Returning to main menu ...\n\n')

    elif usr_option == len(mainMenu_options):
        sys.exit('\n' + strings.colors('Goodbye, see you!', 1) + '\n')

