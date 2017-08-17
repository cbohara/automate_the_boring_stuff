#!/usr/bin/env python3
import sys

def main(args):
    count = 0
    for row in sys.stdin:
        row = row[:-1]
        row = row.split('|')
        if row[1] == '1' or row[2] == '1':
            # print to stdout > output.txt
            print(row[0] + "|1")
            count += 1

    # store final count
    with open('count.txt','a') as f:
        f.write("Total records: " + str(count) + "\n")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
