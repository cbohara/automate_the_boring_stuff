#!/usr/bin/env python
import sys


def main(args):
    """
    Transform single column of values into a string of values
    """
    try:
        filename = sys.argv[1]
    except IndexError:
        print 'python string_of_values.py [filename]'
    else:
        with open(filename) as f:
            content = f.readlines()
            del content[0]
            content[-1] = content[-1]+"\n"
            content = [x[:-1] for x in content]
            output = ','.join(content)
            print output


if __name__ == "__main__":
    main(sys.argv)
