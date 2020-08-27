"""
Numbers utilities

Functions to manipulate numbers (int and float)

Author: Marcus Moresco Boeno

"""

# Standard library imports
import sys

# Local application imports
from utils import strings


def readOption(txt, numOptions):
    """
    > readOption(txt, numOptions)
        Read user option characterized as a integer.

    > Arguments:
        - txt: String to build an input call;
        - numOptions: Number of options available for the user.
    
    > Output:
        - int: User option.
    """
    # Get user input and return when valid
    while True:
        try:
            option = int(input(txt).strip())
        except KeyboardInterrupt:
            sys.exit("\n\n" + strings.colors("Goodbye, see you!", 1) + "\n")
        except:
            print(strings.colors("[ERROR] Enter a valid option!", 1))
        else:
            if 0 < option < numOptions + 1:
                return option
            print(strings.colors("[ERROR] Enter a valid option!", 1))


def readListIndex(txt, start, end):
    """
    > readListIndex(txt, start, end)
        Read an index of a list (Only positive indexes).

    > Arguments:
        - txt: String to build an input call;
        - start: First valid index
        - end: Last valid index.
    
    > Output:
        - int: User defined index.
    """
    # Get user input and return when valid
    while True:
        try:
            option = int(input(txt).strip())
        except KeyboardInterrupt:
            sys.exit("\n\n" + strings.colors("Goodbye, see you!", 1) + "\n")
        except:
            print(strings.colors("[ERROR] Enter a valid option!", 1))
        else:
            if start <= option <= end:
                return option
            print(strings.colors("[ERROR] Enter a valid option!", 1))


def readFloat(txt):
    """
    > readFloat(txt="Enter a real (float) number: ")
        Read a real (float) number.

    > Arguments:
        - txt: String to build an input call.
    
    > Output:
        - float: User defined real number.
    """
    # Get user input and return when valid
    while True:
        try:
            n = float(input(txt).strip())
        except KeyboardInterrupt:
            sys.exit("\n\n" + strings.colors("Goodbye, see you!", 1) + "\n")
        except:
            print(strings.colors("[ERROR] Enter a valid option!", 1))
        else:
            return n


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
    # Build colorized number
    clr_prefix = "\033[" + str(font)
    if color is not None:
        clr_prefix += ";3" + str(color)
    if background is not None:
        clr_prefix += ";4" + str(background)
    clr_prefix += "m"
    
    # Return results
    return clr_prefix + str(txt) + "\033[m"


