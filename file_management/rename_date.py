import sys
import os
import re
import shutil


def main():
    """Rename files from American MM-DD-YYYY to European DD-MM-YYYY date format."""
    # create regex that will match American MM-DD-YYYY format
    american_date_regex = re.compile(r"""
        ^(.*?)              # all text before the date
        ((0|1)?\d)-         # 2, 02, and 12 are all valid months
        ((0|1|2|3)?\d)-     # 3, 03, and 31 are all valid dates
        ((19|20)\d\d)       # year can be 19xx or 20xx
        (.*?)$              # all text after the date
    """, re.VERBOSE)

    # loop over files in the current working directory

    # skip files without a date

    # get the different parts of the filename

    # form the European DD-MM-YYYY date format

    # get full absolute file paths

    # rename files


if __name__ == "__main__":
    sys.exit(main())
