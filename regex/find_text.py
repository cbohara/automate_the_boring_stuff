import sys
import os
import re


def main(args):
    """Search current directory for input text and print filename to terminal if found."""
    try:
        search_text = sys.argv[1]
    except IndexError:
        print("python3 find_text.py [input text to search for within current directory]")
    else:
        # create regex that represents input text
        search_regex = re.compile(search_text)
        # create list of files in current working directory to check for text
        current_dir = os.getcwd()
        files = os.listdir(current_dir)
        # loop through files and print file name to terminal if it contains text
        for filename in files:
            with open(filename) as f:
                content = f.read()
                if search_regex.findall(content):
                    print(filename)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
