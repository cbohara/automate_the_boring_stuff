import csv
import sys


def read_csv(csv_file):
    """Convert csv file into matrix using csv module."""
    input_file = open(csv_file)
    file_reader = csv.reader(input_file)
    matrix = list(file_reader)
    print(matrix)


def main(script):
    """Read in csv file as specified in command line arg."""
    read_csv(sys.argv[1])

if __name__ == "__main__":
    sys.exit(main(sys.argv))
