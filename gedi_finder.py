
import sys, os
from utils import numbers, strings, classes, downloader
from datetime import datetime

# Available GEDI products and versions
gedi_products_list = ['GEDI01_B', 'GEDI02_A', 'GEDI02_B']
gedi_versions_list = ['001']


# Options for main menu
mainMenu_options = [
        'Search LP_DAAC/NASA for GEDI Granules',
        'Exit system'
    ]


def gedi_finder():
    """
    Create a GEDI Finder request
    """
    while True:
        # Read user inputs to process a query for GEDI HTTPS Links
        usr_product = downloader.gedi_products(gedi_products_list)
        usr_version = downloader.gedi_version(gedi_versions_list)
        usr_bbox = downloader.gedi_bbox()
        
        # Print inputs
        print(
            '\nSelected GEDI Product = {} [{}]'.format(
                usr_product, gedi_products_list[usr_product - 1]
            )
        )
        print(
            'Selected GEDI Version = {} [{}]'.format(
                usr_version, gedi_versions_list[usr_version - 1]
            )
        )
        print(f'Defined Bounding Box = {usr_bbox}')

        while True:
            
            # Ask user to confirm defined search criteria
            conf = str(input('Confirm search [y/n]?').strip().lower())
            
            # Test if user wants to confirm or redefine search criteria
            if conf == 'y' or conf == 'n':
                break
            print(strings.colors('[ERROR] Either confirm or decline [y/n]', 1))
        
        if conf == 'y':
            break
        print('\n ... Redefining terms of the search ...\n')

    # Create request object    
    print('\n ... Creating a Request to LPDAAC/NASA Server ...\n')
    obj = classes.GEDI_request(
        p = gedi_products_list[usr_product - 1], 
        v = gedi_versions_list[usr_version - 1], 
        bbox = usr_bbox
        )

    # Calls request - Use LP DAAC GEDI-Finder to define files to download
    gedi_data = obj.process_request()
    print('... Requested Successfully Completed ...')

    # ---- Comment this code if you want the full URL download link ---------- #
    gedi_data = [f[57:] for f in gedi_data]
    # ------------------------------------------------------------------------ #
    
    # Save HTTPS Links as a Text file
    print('... Saving GEDI Granules links to a text file (".txt") ...')

    # Creating output file name
    dt = datetime.now()
    ymd = str(dt.now().year).zfill(4) + str(dt.now().month).zfill(2)
    ymd += str(dt.now().day).zfill(2)
    hms = str(dt.now().hour).zfill(2) + str(dt.now().minute).zfill(2)
    hms += str(dt.second).zfill(2) 
    #
    out_file = 'gedi_finder_links' + os.sep + ymd + '_' + hms + '_'
    out_file += gedi_products_list[usr_product - 1] + '_'
    out_file += gedi_versions_list[usr_version - 1] + '_'
    out_file += '_'.join([str(int(n)) for n in usr_bbox]) + '.txt'

    # Create results directory if it does not exist 
    if not os.path.exists('gedi_finder_links'):
        os.makedirs('gedi_finder_links')
    
    # Save resutls to out_file
    with open(out_file, 'a+') as f:
        f.write('\n# --- GEDI Finder Results\n')
        f.write(f'\nGEDI Level: {gedi_products_list[usr_product - 1]}\n')
        f.write(f'GEDI Version: {gedi_versions_list[usr_version - 1]}\n')
        f.write(f'User-defined Bounding Box: {usr_bbox}\n\n')
        f.write('# --- Links\n\n')        
        for pos, gedi_file in enumerate(gedi_data):
            if pos == len(gedi_data) - 1:
                f.write(f'{gedi_file}\n')
            else:
                f.write(f'{gedi_file},\n')


def main():
    
    # Print a greeting!
    strings.greeting_gediFinder()

    # Print options of actions for the user to select 
    for pos, options in enumerate(mainMenu_options):
        print(
            '{} - {}'.format(
                strings.colors(pos+1, 3), strings.colors(options, 2)
            )
        )
    
    while True:

        # Identifying next action
        usr_option = numbers.readOption(
            'Select an option: ', 
            len(mainMenu_options)
        ) 
        
        # Either make a gedi finder request or exit the system
        if usr_option == 1:
            gedi_finder()
        elif usr_option == 2:
            sys.exit('\n' + strings.colors('Goodbye, see you!', 1) + '\n')
        
        


if __name__ == '__main__':
    main()