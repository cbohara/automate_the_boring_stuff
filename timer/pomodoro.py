#!/usr/bin/env python3
import sys
import os
import time


def main(args):
    """Build simple pomodoro timer."""
    try:
        work_min = int(sys.argv[1])
        rest_min = int(sys.argv[2])
    except IndexError:
        print("python3 pomodoro.py [work duration] [rest duration]")
    else:
        # start working
        os.system('say "Start working"')
        time.sleep(work_min*60)
        # stop working and start rest
        os.system('say "Break time"')
        time.sleep(rest_min*60)
        os.system('say "Study session over"')


if __name__ == "__main__":
    sys.exit(main(sys.argv))
