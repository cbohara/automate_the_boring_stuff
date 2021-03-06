import sys
import re
import csv


def main(script):
    """Grab necessary data from txt file and save into csv file."""
    # read in txt file that contains results 
    txt_file = open("output_T1L3.txt")
    txt_content = txt_file.read()
    txt_file.close()

    # create regex and create list of dates 
    timestamp_regex = re.compile(r'\d\d\d\d-\d\d-\d\d\s\d\d\:\d\d\:\d\d\.\d\d\d\d\d\d')
    timestamps = timestamp_regex.findall(txt_content)

    # find result times
    results_regex = re.compile(r'\[(.+?)\]')
    results = results_regex.findall(txt_content)

    # create matrix of result times
    matrix = []
    for result in results:
        result_list = [int(num) for num in result.split(',')]
        matrix.append(result_list)

    # input corresponding timestamp at the beginning of results row 
    for i, row in enumerate(matrix):
        row.insert(0, timestamps[i])

    # create csv file and write matrix to csv file
    with open("/home/cohara/github/automate/data/results.csv", "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Timestamp"] + list(range(1, len(matrix[0])+1)))
        for row in matrix:
            writer.writerow(row)

    csv_file.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
