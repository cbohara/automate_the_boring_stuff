import sys
import csv
from datetime import datetime as dt
from datetime import timedelta


def time_series(start_date, start_time, end_date, end_time, interval):
    """Create list of times to be found in grafana csv."""
    # convert input time strings into datetime object
    start_time = dt.strptime(start_date + start_time, '%Y-%m-%d%H:%M:%S')
    end_time = dt.strptime(end_date + end_time, '%Y-%m-%d%H:%M:%S')
    # store time series
    times = []
    # iterate from start time to end time and append to times list 
    current = start_time
    while current <= end_time:
        times.append(current)
        current += timedelta(seconds=int(interval))
    # return times that will be searched in grafana csv
    time_series = []
    # change time format into string that will be serched in grafana csv
    for time in times:
        back_to_string = dt.strftime(time, '%Y-%m-%d%H:%M:%S')
        grafana_string = back_to_string[:10] + "T" + back_to_string[10:] + ".000Z"
        time_series.append(grafana_string)
    return time_series


def node_names(matrix):
    """Return list of node names."""
    nodes = []
    for row in matrix:
        if row[0] not in nodes:
            nodes.append(row[0])
    del(nodes[0])
    return nodes


def convert_KiB_to_GiB(memory):
    """Convert KiB to GiB and round to two decimal places."""
    return round((memory / 1048576), 2)


def filter_for_times(matrix, times):
    """Return matrix containing node memory for times specified in time series."""
    output_matrix = []
    node_names = []
    for time in times:
        for row in matrix:
            if str(time) in str(row[1]):
                if row[2] != 'null':
                    row[2] = convert_KiB_to_GiB(float(row[2]))
                output_matrix.append(row)
    return output_matrix


def filter_by_node(matrix, nodes):
    # add times for node to its own array, and then add to final output matrix
    final_output = []
    for node in nodes:
        node_array =[node]
        for row in matrix:
            if node == row[0]:
                node_array.append(row[2])
        final_output.append(node_array)
    return final_output


def main(script):
    """Create csv file containing node memory data for specific times."""
    try:
        # ensure user entered csv file and timestamp
        csv_file = sys.argv[1]
        start_date = sys.argv[2]
        start_time = sys.argv[3]
        end_date = sys.argv[4]
        end_time = sys.argv[5]
        interval = sys.argv[6]
    except IndexError:
        print('python3 app_execution_memory_csv.py [csv_file] [start_date] [start_time] [end_date] [end_time] [increment (seconds)]')
    else:
        # read in grafana csv file
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input, delimiter=';')
            # create matrix from csv file
            matrix = [line for line in file_reader]

        # list of times that need to be found in grafana csv 
        times = time_series(start_date, start_time, end_date, end_time, interval)
        # list of nodes 
        nodes = node_names(matrix)
        # find node data for times in time_series
        filtered_matrix = filter_for_times(matrix, times)
        # need to get filtered matrix into desired output format
        final_output = filter_by_node(filtered_matrix, nodes)
        # write to csv file
        with open('data/memory_per_node_at_' + start_time + '.csv', 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=',', quotechar='', quoting=csv.QUOTE_NONE)
            file_writer.writerows(final_output)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
