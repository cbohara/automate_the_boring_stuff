#! /usr/bin/env python3

import sys
import pyperclip

PASSWORDS = {'account_name': ['user_name', 'password']}


def main(script):
    try:
        account = sys.argv[1]
    except IndexError:
        print('specify account name - python pw.py [account]')
    else:
        if account or account.title() in PASSWORDS:
            pyperclip.copy(PASSWORDS[account][1])
            print('Password for ' + account + ' copied to clipboard.')
        else:
            print('There is no account named ' + account)


if __name__ == '__main__':
    main(sys.argv)
