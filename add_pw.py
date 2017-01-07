"""Add new account to pw.csv and generate a strong password, which will be added
to clipboard to make it easier to make new login accounts."""
import sys
import csv
import pyperclip
import random
import string

def generate_pw():
    """Generate strong password to add to csv file and clipboard."""
    chars = string.ascii_letters + string.digits + '!@#$%^&*()'
    password = ''.join(random.choice(chars) for i in range(16))
    pyperclip.copy(password)
    print('Password copied to clipboard.')
    return password

def main(script):
    try:
        # ensure user entered account name and user name
        account_name = sys.argv[1]
        user_name = sys.argv[2]
    except IndexError:
        print('specify account name and user name- python add_pw.py [account name] [user name]')
    else:
        # read in csv file
        pw_file = open('data/pw.csv')
        pw_object = csv.reader(pw_file)

        # ensure account does not already exist in pw.csv
        for row in pw_object:
            if row[0] == account_name:
                print('Account already exists.')
                break
        # append account name, user name, and password generated by function
        else:
            with open('data/pw.csv', 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                password = generate_pw()
                writer.writerow([account_name, user_name, password])


if __name__ == '__main__':
    main(sys.argv)
