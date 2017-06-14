#!/usr/local/bin/python3
import sys
import shelve
import pyperclip


def main(args):
    """Save and load pieces of text to the clipboard."""
    try:
        command = sys.argv[1].lower()
        key = sys.argv[2].lower()
    except IndexError:
        print("python3 mcb.py [command] [specify key or all]")
    else:
        with shelve.open('multiclipboard') as mcb_shelf:
            if command == 'save':
                mcb_shelf[key] = pyperclip.paste()

            if command == 'load':
                pyperclip.copy(mcb_shelf[key])

            if command == 'list':
                pyperclip.copy(str(list(mcb_shelf.items())))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
