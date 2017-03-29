import sys
import csv


def main(script):
    """Convert csv file from one delimiter to another."""
    try:
        # ensure user entered input csv file, current delimiter, and desired delimiter
        csv_file = sys.argv[1]
        current_delimiter = sys.argv[2]
        desired_delimiter = sys.argv[3]
    except IndexError:
        print('python3 convert_csv_delimiter.py [file_path] [\current delimiter] [\desired delimiter]')
    else:
        # read in csv file
        with open(csv_file, 'r') as file_input:
            file_reader = csv.reader(file_input, delimiter=current_delimiter)
            file_contents = [line for line in file_reader]

        # overwrite csv file with desired delimiter
        with open(csv_file, 'w') as file_output:
            file_writer = csv.writer(file_output, delimiter=desired_delimiter, quotechar='', quoting=csv.QUOTE_NONE)
            file_writer.writerows(file_contents)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
