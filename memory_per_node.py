import sys
import csv


def node_per_timestamp(matrix, timestamp):
    """Filter out input matrix so only one row for each node is returned."""
    for row in matrix:
        if timestamp in row[2]:
            print(timestamp)


def main(script):
    """Find unique rows in csv file."""
    try:
        # ensure user entered csv file and timestamp
        csv_file = sys.argv[1]
        timestamp = sys.argv[2]
    except IndexError:
        print('python3 unique_rows.py [csv_file] [timestamp]')
    else:
        # read in csv file
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input)
            # create matrix from csv file
            matrix = [line for line in file_reader]

    node_per_timestamp(matrix, timestamp)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
