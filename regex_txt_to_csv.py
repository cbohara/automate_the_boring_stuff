import sys
import re


def main(script):
    """Grab data from txt file to save in csv file."""
    # read in txt file that contains times
    txt_file = open("data/output_L13T.txt")
    txt_content = txt_file.read()
    txt_file.close()

    # create regex and create list of dates 
    timestamp_regex = re.compile(r'\d\d\d\d-\d\d-\d\d\s\d\d\:\d\d\:\d\d\.\d\d\d\d\d\d')
    timestamps = timestamp_regex.findall(txt_content)
    print(timestamps)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
