import csv
import sys


def view_reader(file_reader):
    for row in file_reader:
        print("Row #" + str(file_reader.line_num) + " " + str(row))


def create_matrix(file_reader):
    matrix = list(file_reader)
    for row, col in enumerate(matrix):
        print(matrix[row])


def main(script):
    """Read in csv file specified in command line argv."""
    try:
        csv_file = sys.argv[1]
    except:
        print("python3 auto_csv.py [csv file]")
    else:
        input_file = open(csv_file)
        file_reader = csv.reader(input_file)
        #view_reader(file_reader)
        create_matrix(file_reader)


if __name__ == "__main__":
    main(sys.argv)
