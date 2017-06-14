#!/usr/local/bin/python3
# mcb.pyw - Saves and loads pieces of text to the clipboard
# Usage: 
#   py.exe mcb.pyw save [key] - saves clipboard text to key
#   py.exe mcb.pyw load [key] - loads key content to the clipboard
#   py.exe mcb.pyw list - loads all content to the clipboard

import sys
import shelve
import pyperclip


def main(args):
    """Save and load pieces of text to the clipboard."""
    # save command line arguments
    command = sys.argv[1].lower()
    try:
        key = sys.argv[2].lower()
    except IndexError:
        pass

    # open shelf file to save variables into binary file in order to load from the hard drive later
    mcb_shelf = shelve.open('mcb')

    if command == 'save':
        mcb_shelf[key] = pyperclip.paste()

    if command == 'load':
        pyperclip.copy(mcb_shelf[key])

    if command == 'list':
        pyperclip.copy(str(list(mcb_shelf.keys())))


    mcb_shelf.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
