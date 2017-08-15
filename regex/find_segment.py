#!/usr/bin/env python

import sys
import re
import time


def main(args):
    """Check if segment exists in file."""
    try:
        target_segments = sys.argv[1]
    except IndexError:
        print "\ngzip -dc <FILENAME>.txt.gz | python find_segment.py SEGMENT1=T,SEGMENT2=T,SEGMENT3=T\n"
    else:
        start_time = time.time()
        target_list = target_segments.split(',')
        target_regex = [re.compile('\\b' + x + '\\b') for x in target_list]

        stdin = sys.stdin.read()
        file_data = stdin.split("|")
        del file_data[0]

        found_segments = []
        segment_found = False
        for segment in file_data:
            for regex in target_regex:
                if regex.search(segment):
                    segment_found = True
                    found_segments.append(segment)

        duration = round(((time.time() - start_time)/60), 0)
        print duration + " seconds"
        print found_segments
        print segment_found

if __name__ == "__main__":
    sys.exit(main(sys.argv))
