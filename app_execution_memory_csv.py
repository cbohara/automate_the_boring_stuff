import sys
import csv
from datetime import datetime as dt
from datetime import timedelta


def times(date, start_time, end_time, interval):
    """Create list of times to be found in grafana csv."""
    # convert input time strings into datetime object
    start_time = dt.strptime(date + start_time, '%Y-%m-%d%H:%M:%S')
    end_time = dt.strptime(date + end_time, '%Y-%m-%d%H:%M:%S')
    # store time series
    times = []
    # iterate from start time to end time and append to times list 
    current = start_time
    while current <= end_time:
        current += timedelta(seconds=600)
        back_to_string = dt.strftime(current, '%Y-%m-%d%H:%M:%S')
        grafana_string = back_to_string[:10] + "T" + back_to_string[10:] + ".000Z"
        times.append(grafana_string)
    return times


def convert_KiB_to_GiB(memory):
    """Convert KiB to GiB and round to two decimal places."""
    return round((memory / 1048576), 2)


def memory_at_time(matrix, time_series):
    """Return matrix containing node info at timestamp."""
    output_matrix = []
    node_names = []
    for time in time_series:
        for row in matrix:
            if str(time) in str(row[1]):
                if row[2] != 'null':
                    row[2] = convert_KiB_to_GiB(float(row[2]))
#                row[1] = row[2]
#                del(row[2])
                output_matrix.append(row)
    for row in output_matrix:
        print(row)
    return output_matrix


def main(script):
    """Create csv file containing node memory data for specific times."""
    try:
        # ensure user entered csv file and timestamp
        csv_file = sys.argv[1]
        date = sys.argv[2]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        interval = sys.argv[5]
    except IndexError:
        print('python3 app_execution_memory_csv.py [csv_file] [date] [start_time] [end_time] [time_increment]')
    else:
        # read in grafana csv file
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input, delimiter=';')
            # create matrix from csv file
            matrix = [line for line in file_reader]

        # generate header including list of times that need to be found in grafana csv 
        time_series = times(date, start_time, end_time, interval)

        # find node data for times in time_series
        output_matrix = memory_at_time(matrix, time_series)

        # write to csv file
        with open('data/memory_per_node_at_' + start_time + '.csv', 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=',', quotechar='', quoting=csv.QUOTE_NONE)
            file_writer.writerows(output_matrix)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
