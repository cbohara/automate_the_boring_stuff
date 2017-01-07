import sys
import csv
import pyperclip


def main(script):
    """Retrieve password from csv file."""
    try:
        # ensure user entered account name
        account_name = sys.argv[1]
    except IndexError:
        print('specify account name - python get_pw.py [account name]')
    else:
        # read in csv file
        pw_file = open('data/pw.csv')
        pw_object = csv.reader(pw_file)
        # get password if account exists in pw.csv
        for row in pw_object:
            if row[0] == account_name:
                pyperclip.copy(row[2])
                print('Password for ' + account_name + ' copied to clipboard.')
            else:
                print('There is no account named ' + account)


if __name__ == '__main__':
    main(sys.argv)
