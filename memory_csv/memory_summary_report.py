import sys
import csv


def convert_KiB_to_GiB(memory):
    """Convert KiB to GiB and round to two decimal places."""
    return round((memory / 1048576), 2)


def approx_total_cached_memory(timestamp_matrix):
    """Calculate total cached memory across all nodes."""
    total_cached_memory = 0
    for row in timestamp_matrix:
        if 'cached' in row[0] and row[2] != 'null':
            total_cached_memory += float(row[2])
        else:
            continue
    return convert_KiB_to_GiB(total_cached_memory)


def approx_total_used_memory(timestamp_matrix):
    """Calculate total cached memory across all nodes."""
    total_used_memory = 0
    for row in timestamp_matrix:
        if 'used' in row[0] and row[2] != 'null':
            total_used_memory += float(row[2])
        else:
            continue
    return convert_KiB_to_GiB(total_used_memory)


def main(script):
    """Create report of node memory useage."""
    try:
        # ensure user specifies csv file for input
        csv_file = sys.argv[1]
    except IndexError:
        print('python3 memory_summary_report.py [csv_file]')
    else:
        # read in csv file containing node data for a given timestamp
        with open(csv_file, 'r') as input_file:
            file_reader = csv.reader(input_file)
            # create matrix from csv file
            timestamp_matrix = [line for line in file_reader]

        # store timestamp for labeling output file
        timestamp = timestamp_matrix[1][1]
        # convert total memory available per node to GiB
        memory_available_per_node = timestamp_matrix[1][2]
        if memory_available_per_node != 'null':
           memory_available_per_node = convert_KiB_to_GiB(float(memory_available_per_node))

        # output results to text file
        output_file = open('data/summary_report_'+timestamp+'.txt', 'w')
        output_file.write('Timestamp: ' + timestamp + '\n')
        output_file.write('Memory available per node: ' + str(memory_available_per_node) + ' GiB \n')
        output_file.write('Approx. total cached memory: ' + str(approx_total_cached_memory(timestamp_matrix)) + ' GiB \n')
        output_file.write('Approx. total used memory: ' + str(approx_total_used_memory(timestamp_matrix)) + ' GiB \n')


if __name__ == "__main__":
    sys.exit(main(sys.argv))
