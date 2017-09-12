#!/usr/bin/env python3
import os
import sys
import csv
from fdfgen import forge_fdf


def fin_translation(fin_value):
    if fin_value == '1':
        return ('single', 'X')
    elif fin_value == '2':
        return ('double', 'X')
    elif fin_value == '3':
        return ('thruster', 'X')
    elif fin_value == '4':
        return ('quad', 'X')
    elif fin_value == '5':
        return ('finfive', 'X')
    else:
        return ('otherfin', 'X')


def tail_translation(tail_value):
    if tail_value == 'square':
        return ('square', 'X')
    elif tail_value == 'squash':
        return ('squash', 'X')
    elif tail_value == 'round':
        return ('round', 'X')
    elif tail_value == 'round pin':
        return ('round pin', 'X')
    elif tail_value == 'pin':
        return ('pin', 'X')
    elif tail_value == 'swallow':
        return ('swallow', 'X')
    elif tail_value == 'fish':
        return ('fish', 'X')
    else:
        return ('tail', tail_value)


def process_csv(csv_file):
    csv_data = csv.reader(open(csv_file))
    header = []
    output_data = []
    for row_number, row in enumerate(csv_data):
        if row_number == 0:
            continue
        if row_number == 1:
            header = row
            header = [word.lower().strip() for word in header]
            continue

        key_value_tuples = []
        for i in range(len(header)):
            value = row[i].strip()
            if header[i] == 'fins':
                fin_tuple= fin_translation(value)
                if fin_tuple[0] == 'otherfin':
                    key_value_tuples.append(fin_tuple)
                    key_value_tuples.append((header[i], value))
                else:
                    key_value_tuples.append(fin_tuple)
            elif header[i] == 'tail':
                tail_tuple = tail_translation(value.lower())
                key_value_tuples.append(tail_tuple)
            elif header[i] == 'or':
                if value:
                    key_value_tuples.append('or', 'X')
                    key_value_tuples.append('ortext', value)
            else:
                key_value_tuples.append((header[i], value))
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
