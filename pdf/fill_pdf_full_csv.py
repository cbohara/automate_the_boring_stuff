#!/usr/bin/env python3
import os
import sys
import csv
from fdfgen import forge_fdf


def process_csv(csv_file):
    csv_data = csv.reader(open(csv_file))
    header = []
    output_data = []
    for row_number, row in enumerate(csv_data):
        if row_number == 0:
            continue
        if row_number == 1:
            header = row
            header = [word.lower() for word in header]
            continue
        key_value_tuples = []
        for i in range(len(header)):
            key_value_tuples.append((header[i], row[i]))
        output_data.append(key_value_tuples)
    return output_data


def fill_pdf_template(row, pdf_template):
    tmp_file = "tmp.fdf"
    fdf = forge_fdf("", row, [], [], [])
    with open(tmp_file, "wb") as fdf_file:
        fdf_file.write(fdf)
    output_file = "{0}.pdf".format(row[3][1])
    cmd = "pdftk '{0}' fill_form '{1}' output '{2}' dont_ask".format(pdf_template, tmp_file, output_file)
    os.system(cmd)
    os.remove(tmp_file)


def main(args):
    """
    Batch fill pdf template with data from csv file
    """
    try:
        pdf_template = sys.argv[1]
        csv_file = sys.argv[2]
    except IndexError:
        print("python3 fill.py pdf_template csv_file")
    else:
        data = process_csv(csv_file)
        for row in data:
            fill_pdf_template(row, pdf_template)
            print("Generated {0}.pdf".format(row[3][1]))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
