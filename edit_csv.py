import sys
import csv


def partial_url(row, index):
    """Determine if value is only a partial url and needs to be a full url."""
    if len(row[index]) != 0:
        if row[index][0] == '/':
            return True
    return False

def prepend_url(string):
    """Return full url."""
    s_to_l = list(string)
    s_to_l.insert(0, 'https://www.nutanix.com')
    full_url = ''.join(s_to_l)
    return full_url


def main(script):
    """Script for updating url fields in CSV file."""
    original_file = open('data/customers_original.csv')
    original_object = csv.reader(original_file)

    output_csv = []
    for row in original_object:
        if partial_url(row, 4):
            full_url = prepend_url(row[4])
            row[4] = full_url

        if partial_url(row, 5):
            full_url = prepend_url(row[5])
            row[5] = full_url

        if partial_url(row, 6):
            full_url = prepend_url(row[6])
            row[6] = full_url

        output_csv.append(row)

    with open('data/customers_edited.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        for row in output_csv:
            writer.writerow(row)

    original_file.close()


if __name__ == '__main__':
    main(sys.argv)
