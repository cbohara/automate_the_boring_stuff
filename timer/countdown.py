#!/usr/local/bin/python3
import sys
import os
import time
import subprocess


def main(args):
    """A simple countdown script with an alarm at the end."""
    try:
        seconds = int(sys.argv[1])
    except IndexError:
        print("python3 countdown.py [time in seconds]")
    else:
        while seconds > 0:
            print(seconds)
            time.sleep(1)
            seconds = seconds - 1

        os.system("say 'The countdown is finished'")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
