#!/usr/bin/env python

import sys

def main(args):
    try:
        prefix = sys.argv[1]
    except IndexError:
        print "cat file_input.csv | python add_prefix.py [a or g] > file_output.txt"
    else:
        # read in from cat file_input.csv
        for line in sys.stdin:
            sys.stdout.write(prefix+"."+line)


if __name__ == "__main__":
    main(sys.argv)
