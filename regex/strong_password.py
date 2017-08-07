#!/usr/bin/env python3
import sys
import re


def main(args):
    """Determine if password is strong - 8+ chars, upper and lowercase chars, 1+ int, 1+ special char."""
    try:
        password = sys.argv[1]
    except IndexError:
        print("python3 strong_password.py [password]")
    else:
        upper = re.compile(r'[A-Z]')
        lower = re.compile(r'[a-z]')
        digit = re.compile(r'\d+')
        special = re.compile(r'\W+')

        if upper.findall(password) and lower.findall(password) and digit.findall(password) and special.findall(password):
            print(password, 'is a strong password.')

if __name__ == "__main__":
    sys.exit(main(sys.argv))
