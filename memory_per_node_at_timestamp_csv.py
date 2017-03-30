import sys
import csv


def convert_KiB_to_GiB(memory):
    """Convert KiB to GiB and round to two decimal places."""
    return round((memory / 1048576), 2)


def nodes_at_timestamp(matrix, timestamp):
    """Return matrix containing node info at timestamp."""
    timestamp_matrix = []
    for row in matrix:
        if timestamp in row[1]:
            if row[2] != 'null':
                row[2] = convert_KiB_to_GiB(float(row[2]))
            timestamp_matrix.append(row)
    return timestamp_matrix


def main(script):
    """Create csv file containing node memory info for specific time."""
    try:
        # ensure user entered csv file and timestamp
        csv_file = sys.argv[1]
        date = sys.argv[2]
        time = sys.argv[3]
    except IndexError:
        print('python3 memory_per_node.py [csv_file] [timestamp]')
    else:
        # read in grafana csv file
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input, delimiter=';')
            # create matrix from csv file
            matrix = [line for line in file_reader]

        # create matrix only containing info for specific timestamp
        timestamp_matrix = nodes_at_timestamp(matrix, timestamp)

        # write to csv file
        with open('data/memory_per_node_at_'+timestamp+'.csv', 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=',', quotechar='', quoting=csv.QUOTE_NONE)
            file_writer.writerow(['Node', 'Time','Memory(GiB)'])
            file_writer.writerows(timestamp_matrix)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
