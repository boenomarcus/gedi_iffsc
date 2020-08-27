"""
Strings utilities

Functions to manipulate strings

Author: Marcus Moresco Boeno

"""

def colors(txt, color=None, background=None, font=0):
    """
    > colors(txt, color=None, background=None, font=0)
        ANSI string colors on terminal.

    > Arguments:
        - txt: String to be colored;
        - color: ANSI Color (default = None);
        - background: ANSI Background color (default = None);
        - font: ANSI Style (default = 0).
    
    > Output:
        - str: String colorized.
    """
    # Build colorized string
    clr_prefix = "\033[" + str(font)
    if color is not None:
        clr_prefix += ";3" + str(color)
    if background is not None:
        clr_prefix += ";4" + str(background)
    clr_prefix += "m"
    
    # Return results
    return clr_prefix + str(txt) + "\033[m"


def yes_no_input(txt):
    """
    > yes_no_input(txt)
        Collect yes or no input from user.

    > Arguments:
        - txt: String to build an input call.
    
    > Output:
        - str: User yes or no response.
    """
    # Get user input and return when valid
    while True:
        a = input(txt)
        if a in "YyNn":
            return a
        print(colors("[ERROR] Confirm or deny [y/n]!", 1))


def greeting_olms():
    """
    > greeting_olms()
        Print a greetings message on system execution.

    > Arguments:
        - No arguments.
    
    > Output:
        - No outputs.
    """
    print("\n" + "-="*40 + "\n" + "-"*80)
    print(f"{'Welcome to OLMS - Orbital LiDAR Management System!':^80}")
    print("-"*80 + "\n" + "-="*40 + "\n")

