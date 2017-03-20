import csv
import sys


def print_rows(file_reader):
    for row in file_reader:
        print("Row #" + str(file_reader.line_num) + " " + str(row))


def main(script):
    """Read in csv file specified in command line argv."""
    try:
        csv_file = sys.argv[1]
    except:
        print("python3 auto_csv.py [csv file]")
    else:
        input_file = open(csv_file)
        file_reader = csv.reader(input_file)
        print_rows(file_reader)


if __name__ == "__main__":
    main(sys.argv)
