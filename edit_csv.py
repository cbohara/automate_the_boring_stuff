"""Script for updating fields in CSV file."""
import sys
import csv


def prepend_url(string):
    s_to_l = list(string)
    s_to_l.insert(0, 'https://www.nutanix.com')
    full_url = ''.join(s_to_l)
    return full_url


def main(script):
    original_file = open('customers_original.csv')
    original_object = csv.reader(original_file)

    output_csv = []

    for row in original_object:
        if len(row[4]) != 0:
            if row[4][0] == '/':
                full_url = prepend_url(row[4])
                row[4] = full_url

        if len(row[5]) != 0:
            if row[5][0] == '/':
                full_url = prepend_url(row[5])
                row[5] = full_url

        if len(row[6]) != 0:
            if row[6][0] == '/':
                full_url = prepend_url(row[6])
                row[6] = full_url

        output_csv.append(row)

    original_file.close()


if __name__ == '__main__':
    main(sys.argv)
