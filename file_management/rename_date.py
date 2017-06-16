#!/usr/local/bin/python3
import sys
import os
import re
import shutil


def main():
    """Rename files from American MM-DD-YYYY to international DD-MM-YYYY date format."""
    # create regex that will match American MM-DD-YYYY format
    american_date_regex = re.compile(r"""
        ^(.*?)              # all text before the date
        ((0|1)?\d)-         # 2, 02, and 12 are all valid months
        ((0|1|2|3)?\d)-     # 3, 03, and 31 are all valid dates
        ((19|20)\d\d)       # year can be 19xx or 20xx
        (.*?)$              # all text after the date
    """, re.VERBOSE)

    # loop over files in the current working directory
    for filename in os.listdir("."):
        match = american_date_regex.search(filename)

        # if the file doesn't contain a date continue through for loop 
        if match == None:
            continue

        # get the different parts of the filename
        before = match.group(1)
        month = match.group(2)
        day = match.group(4)
        year = match.group(6)
        after = match.group(8)

        # form the international DD-MM-YYYY date format 
        intl_filename = before + day + "-" + month + "-" + year + after

        # get full absolute file paths
        abs_dir = os.path.abspath(".")
        filename = os.path.join(abs_dir, filename)
        intl_filename = os.path.join(abs_dir, intl_filename)

        # rename the file
        print("Renaming {} to {}".format(filename, intl_filename))
#        shutil.move(filename, intl_filename)


        # rename files


if __name__ == "__main__":
    sys.exit(main())
