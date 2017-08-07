#!/usr/bin/env python

import sys
import re


def main(args):
    """ Retreive threshold values to paste into hadoop config """
    try:
        target_segment = sys.argv[1]
    except IndexError:
        print "\ngzip -dc /mnt/tmp/mf/<FILENAME>.txt.gz | head -n 1 | python threshold.py [segment]\n"
    else:
        # read in from `gzip -dc <FILENAME> | head -n 1 | `
        stdin = sys.stdin.read()
        # create list of segments
        header = stdin.split("|")
        # remove column title
        del header[0]

        # using \b boundary to ensure that only the exact target statement matches
        segment_regex = re.compile('\\b' + target_segment + '\\b')

        # flag to ensure match is found
        segment_found = False
        # store threshold values
        threshold = []
        for segment in header:
            if segment_regex.search(segment):
                segment_found = True
                threshold.append(1)
            else:
                threshold.append(0)

        if segment_found:
            threshold_output = ""
            for value in threshold:
                threshold_output += str(value) + ","

            # get rid of last comma
            final = threshold_output[:-1]
            print final

        else:
            print "\nError: Segment not found in header\n"


if __name__ == "__main__":
    sys.exit(main(sys.argv))
