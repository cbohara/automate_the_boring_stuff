#!/usr/bin/env python
import sys
import re


def main(args):
    """ Retreive threshold values to paste into hadoop config """
    try:
        input_targets = sys.argv[1]
    except IndexError:
        print "\ngzip -dc <FILENAME>.txt.gz | head -1 | python threshold.py segment1,segment2,segment3\n"
    else:
        # transform target segments to one regex for efficiency
        input_list = input_targets.split(',')
        regex_boundary = ["\\b" + x + "\\b" for x in input_list]
        regex_string = "|".join(regex_boundary)
        regex = re.compile(regex_string)

        stdin = sys.stdin.read()
        # create list of segments from input file
        header = stdin.split("|")
        # remove column title
        del header[0]


        # flag to ensure match is found to avoid unnecessary compute
        segment_found = False
        # store threshold values
        threshold = []
        for segment in header:
            if regex.search(segment):
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
