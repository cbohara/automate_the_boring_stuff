import sys
import os
import re


def main(args):
    """Search current directory for input text and change if found"""
    try:
        search_text = sys.argv[1]
        desired_text = sys.argv[2]
    except IndexError:
        print("python3 find_text.py [input text to search for within current directory] [desired text]")
    else:
        # create list of files in current working directory to check for text
        current_dir = os.getcwd()
        files = os.listdir(current_dir)
        # loop through files and print file name to terminal if it contains text
        for filename in files:
            lines = []
            with open(filename) as fin:
                for line in fin:
                    if search_text in line:
                        replaced_line = line.replace(search_text, desired_text)
                        lines.append(replaced_line)
                    else:
                        lines.append(line)
            with open(filename, 'w') as fout:
                for line in lines:
                    fout.write(line)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
