#!/usr/local/bin/python3
import sys
import os
import zipfile


def main(args):
    """Copies an entire folder and its contents into a zipfile whose filename increments."""
    try:
        folder = sys.argv[1]
    except IndexError:
        print("python3 backup_to_zip.py [folder in cwd]")
    else:
        folder = os.path.abspath(folder)

        # determine the next increment value to attach to filename
        n = 1
        while True:
            zipfile_name = os.path.basename(folder) + str(n) + ".zip"

            # if the filename does not exist yet, this will be the filename n
            if not os.path.exists(zipfile_name):
                break
            # otherwise increment n and check again
            n += 1

if __name__ == "__main__":
    sys.exit(main(sys.argv))
