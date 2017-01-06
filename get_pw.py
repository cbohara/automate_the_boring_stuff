"""Script for password retrieval from basic dictionary with key specifying the
account name and a list containing the username and password."""

#! /usr/bin/env python3

import sys
import pyperclip


PASSWORDS = {'account': ['user', 'password']}


def main(script):
    try:
        account = sys.argv[1]
    except IndexError:
        print('specify account name - python get_pw.py [account name]')
    else:
        if account in PASSWORDS.keys():
            pyperclip.copy(PASSWORDS[account][1])
            print('Password for ' + account + ' copied to clipboard.')
        else:
            print('There is no account named ' + account)


if __name__ == '__main__':
    main(sys.argv)
