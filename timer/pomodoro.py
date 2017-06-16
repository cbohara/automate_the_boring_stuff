#!/usr/local/bin/python3
import sys
import time
import subprocess


BELL = "/Users/cbohara/code/automate/timer/bell.mp3"
THUNDER = "/Users/cbohara/code/automate/timer/thunder.mp3"


def play(duration, sound):
    """Play sound for specified duration of time."""
    cmd = ["open", sound]
    p = subprocess.Popen(cmd)
    time.sleep(duration)
    p.kill()

def main(args):
    """Build simple pomodoro timer."""
    try:
        work_min = int(sys.argv[1])
        rest_min = int(sys.argv[2])
    except IndexError:
        print("python3 pomodoro.py [work duration] [rest duration]")
    else:
        # display text to stdout
        display = lambda x: sys.stdout.write(str(x) + "\n")

        # start timer
        display("Work for {} minutes".format(work_min))
        play(work_min*60, THUNDER)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
