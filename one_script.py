import sys
import csv


def node_at_timestamp(matrix, timestamp):
    """return matrix containing node info at timestamp."""
    timestamp_matrix = []
    for row in matrix:
        print(row[0])
    return timestamp_matrix


def main(script):
    """Create csv file containing node memory info for specific time and append summary data to overall report."""
    try:
        # ensure user entered csv file and timestamp
        csv_file = sys.argv[1]
        timestamp = sys.argv[2]
    except IndexError:
        print('python3 memory_at_timestamp_csv.py [csv_file] [timestamp 00:00:00]')
    else:
        # read in grafana semi-colon delimited csv file 
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input, delimiter=';')
            file_contents = [line for line in file_reader]

        # overwrite grafana csv file with comma delimiter
        with open(csv_file, 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=',')
            file_writer.writerows(file_contents)

        # read in re-formated grafana csv 
        with open(csv_file, 'r') as formatted_csv:
            # read in csv file
            formatted_reader = csv.reader(formatted_csv)
            # create matrix from csv file
            matrix = [line for line in formatted_reader]
            # create matrix only containing info for specific timestamp
            timestamp_matrix = node_at_timestamp(matrix, timestamp)

        # write to csv file
        with open('data/'+timestamp, 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=',', quotechar='', quoting=csv.QUOTE_NONE)
            file_writer.writerows(timestamp_matrix)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
