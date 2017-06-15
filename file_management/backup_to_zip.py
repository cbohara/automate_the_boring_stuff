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
        folder = os.path.relpath(folder)

        # determine the next increment value to attach to filename
        n = 1
        while True:
            zipfile_name = os.path.basename(folder) + str(n) + ".zip"

            # if the filename does not exist yet, this will be the filename n
            if not os.path.exists(zipfile_name):
                break
            # otherwise increment n and check again
            n += 1

        # create zip file
        print("Creating {}".format(zipfile_name))
        with zipfile.ZipFile(zipfile_name, "w") as backup:
            # walk the folder tree and compress the files in each folder
            for foldername, subfolders, filenames in os.walk(folder):
                print("Adding files in {}".format(foldername))
                backup.write(foldername)
                for filename in filenames:
                    print("Adding {}".format(os.path.join(foldername, filename)))
                    backup.write(os.path.join(foldername, filename))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
