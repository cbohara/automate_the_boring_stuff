import sys
import csv


def node_per_timestamp(matrix, timestamp):
    """Filter out input matrix so only row with timestamp in each node is returned."""
    timestamp_matrix = []
    for row in matrix:
        if timestamp in row[1]:
            timestamp_matrix.append(row)
    return timestamp_matrix

def main(script):
    """Create csv file containing node memory info for specific time."""
    try:
        # ensure user entered csv file and timestamp
        csv_file = sys.argv[1]
        timestamp = sys.argv[2]
    except IndexError:
        print('python3 memory_per_node.py [csv_file] [timestamp]')
    else:
        # read in csv file
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input)
            # create matrix from csv file
            matrix = [line for line in file_reader]

        # create matrix only containing info for specific timestamp
        timestamp_matrix = node_per_timestamp(matrix, timestamp)

        # write to csv file
        with open('data/'+timestamp, 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=',', quotechar='', quoting=csv.QUOTE_NONE)
            file_writer.writerows(timestamp_matrix)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
