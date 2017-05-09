import sys
import csv
from datetime import datetime as dt
from datetime import timedelta


def convert_KiB_to_GiB(memory):
    """Convert KiB to GiB and round to two decimal places."""
    return round((float(memory) / 1048576), 2)


def node_names(matrix):
    """Return list of node names."""
    nodes = []
    for row in matrix:
        if row[0] not in nodes:
            nodes.append(row[0])
    del(nodes[0])
    return nodes


def increment_datetime(start_date, start_time, end_date, end_time, interval):
    """Use datetime module to increment time and append to times list."""
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
    return times


def header_times(start_date, start_time, end_date, end_time, window):
    """Format header times to clarify the time window of each memory value."""
    # list of timestamps that need to be included in header 
    times = increment_datetime(start_date, start_time, end_date, end_time, window)
    # change time format to show range of window
    formatted_times = [dt.strftime(time, '%H:%M:%S') for time in times]
    # specify time range of window
    header_times = []
    for index, value in enumerate(formatted_times):
        if index == 0:
            pass
        else:
            time_range = formatted_times[index-1] + "-" + formatted_times[index]
            header_times.append(time_range)
    return header_times


def grafana_times(start_date, start_time, end_date, end_time, frequency):
    """Create list of times to be found in grafana csv."""
    # list of timestamps that need to be found in grafana csv
    times = increment_datetime(start_date, start_time, end_date, end_time, frequency)
    # format times as required to search within grafana csv
    grafana_times = []
    # change time format into string that will be serched in grafana csv
    for time in times:
        dt_to_string = dt.strftime(time, '%Y-%m-%d%H:%M:%S')
        grafana_string = dt_to_string[:10] + "T" + dt_to_string[10:] + ".000Z"
        grafana_times.append(grafana_string)
    return grafana_times


def filter_for_times(matrix, times):
    """Return matrix containing node memory for times specified in time series."""
    output_matrix = []
    node_names = []
    for time in times:
        for row in matrix:
            if str(time) in str(row[1]):
                if row[2] != 'null':
                    row[2] = convert_KiB_to_GiB(row[2])
                else:
                    row[2] = 0
                output_matrix.append(row)
    return output_matrix


def max_per_window(times, frequency, window):
    """Return aggregated list of max values for given time windows."""
    max_per_window = []
    # add node name to beginning of the list
    max_per_window.append(times[0])
    # calculate number of values per window
    num_values = int(window) // int(frequency)
    # start and end of window interval
    start = 1
    end = 1 + num_values
    # traverse list and find the max value in the window and add to list to output
    while end <= len(times):
        max_per_window.append(max(times[start:end]))
        start = end
        end = start + num_values
    return max_per_window


def filter_by_node(matrix, nodes, frequency, window):
    """Return matrix that will be converted into the final csv file."""
    final_matrix = []
    for node in nodes:
        node_array = [node]
        for row in matrix:
            if node == row[0]:
                node_array.append(row[2])
        aggregate_list = max_per_window(node_array, frequency, window)
        final_matrix.append(aggregate_list)
    return final_matrix


def main(script):
    """Create csv file containing node memory data for specific times."""
    try:
        # ensure user required inputs 
        csv_file = sys.argv[1]
        start_date = sys.argv[2]
        start_time = sys.argv[3]
        end_date = sys.argv[4]
        end_time = sys.argv[5]
        frequency = sys.argv[6]
        window = sys.argv[7]
    except IndexError:
        print('python3 memory_streaming_csv.py [csv file] [utc start date ex: 2017-01-01] [utc start time ex: 01:01:01] [utc end date] [utc end time] [data frequency(sec)] [window interval(sec)]')
    else:
        # read in grafana csv file
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input, delimiter=';')
            # create matrix from csv file
            matrix = [line for line in file_reader]

        # list of times that need to be found in grafana csv 
        times = grafana_times(start_date, start_time, end_date, end_time, frequency)
        # list of nodes 
        nodes = node_names(matrix)
        # find node data for times in time_series
        filtered_matrix = filter_for_times(matrix, times)
        # need to get filtered matrix into desired output format
        final_output = filter_by_node(filtered_matrix, nodes, frequency, window)
        # write to csv file
        with open('data/'+start_date+"_"+start_time+'.csv', 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=',', quotechar='', quoting=csv.QUOTE_NONE)
            file_writer.writerow(['Node name', 'Maximum memory value (GiB) in time window'])
            file_writer.writerow([''] + header_times(start_date, start_time, end_date, end_time, window))
            file_writer.writerows(final_output)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
