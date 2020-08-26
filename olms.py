import sys

from utils import strings, numbers, gediTasks


def gedi_menu():
    """
    > gedi_menu()
        GEDI Tasks Main Menu.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs.
    """
    # GEDI Facilities menu
    gediMenu = [
        "GEDI Finder - Search LP_DAAC/NASA Database for GEDI Granules",
        "GEDI Downloader - Download Processed Granules from LPDAAC_NASA",
        "GEDI Storer - Update GEDI MongoDB Database",
        "GEDI Extractor - Extract GEDI Data from MongoDB Database",
        "Return to Main Menu",
        "Exit System"
        ]
        
    # Print options of actions for the user to select 
    print("\n" + "- - " * 20)
    print("\n> GEDI Facilities:\n")
    for pos, options in enumerate(gediMenu):
        print(
            "[{}] {}".format(
                strings.colors(pos+1, 3), strings.colors(options, 2)
                )
            )
        
    # Identifying next action
    gedi_option = numbers.readOption(
        "Select an option: ", 
        len(gediMenu)
        )
    
    if gedi_option == 1:
        # Go to GEDI Finder Menu
        gediTasks.gedi_finder()
    
    elif gedi_option == 2:
        # Go to GEDI Downloader Menu
        gediTasks.gedi_downloader()
    
    elif gedi_option == 3:
        # Go to GEDI Storer Menu
        gediTasks.gedi_storer()
    
    elif gedi_option == 4:
        # Go to GEDI Extractor Menu
        gediTasks.gedi_extractor()
    
    elif gedi_option == 5:
        # Return to Main Menu
        print("\n >> Returning to main menu ...\n")
        print("\n" + "- - " * 20, "\n")
    
    else:
        # Exit system with a goodbye message
        sys.exit("\n" + strings.colors("Goodbye, see you!", 1) + "\n")


def ic2_menu():
    """
    TO BE IMPLEMENTED !!!
    """
    pass


def ic1_menu():
    """
    TO BE IMPLEMENTED !!!
    """
    pass


def main_menu():
    """
    > main_menu()
        OLMS Main Menu.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs.
    """
    while True:
        # Options for main menu
        mainMenu_options = [
            "GEDI - Global Ecosystem Dinamics Investigation",
            "ICESat-2 (ATLAS) - Ice, Cloud and land Elevation Satellite 2",
            "ICESat-1 (GLAS) - Ice, Cloud and land Elevation Satellite 1",
            "Exit system"
            ]
            
        # Print options of actions for the user to select 
        print("> Main Menu")
        print("\n> Select a product to work with:\n")
        for pos, options in enumerate(mainMenu_options):
            print(
                "[{}] {}".format(
                    strings.colors(pos+1, 3), strings.colors(options, 2)
                    )
                )
            
        # Identifying next action
        usr_option = numbers.readOption(
            "Select an option: ", 
            len(mainMenu_options)
            )

        if usr_option == 1:
            # Go to GEDI Menu
            gedi_menu()

        elif usr_option == 2:
            print(strings.colors("\nICESat-2 not implemented yet !!!\n", 1))
            # Go to ICESat-2 Menu
            #ic2_menu()

        elif usr_option == 3:
            print(strings.colors("\nICESat-1 not implemented yet !!!\n", 1))
            # Go to ICESat-1 Menu
            #ic1_menu()

        else:
            # Exit system with a goodbye message
            sys.exit("\n" + strings.colors("Goodbye, see you!", 1) + "\n")


if __name__ == "__main__":
    
    # Print greetings message
    strings.greeting_olms()

    # Initialize System
    main_menu()