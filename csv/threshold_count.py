#!/usr/bin/env python3
import sys

def main(args):
    count = 0
    for row in sys.stdin:
        row = row[:-1]
        row = row.split('|')
        print(row)
        if row[1] == '1' or row[2] == '1':
            count += 1

    print(count)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
