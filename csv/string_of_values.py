#!/usr/bin/env python
import sys


def main(args):
    """
    Transform single column of values into a string of values
    """
    with open('SS-1883.csv') as f:
        content = f.readlines()
        del content[0]
        content = [x[:-1] for x in content]
        output = ','.join(content)
        print output


if __name__ == "__main__":
    main(sys.argv)
