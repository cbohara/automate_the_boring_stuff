import sys
import re


def main(script):
    """ Pull out runtime data from stdout of incomplete Spark job."""
    with open("/home/cohara/github/automate/data/vanilla_stdout.txt") as stdout:
        runtimes = []
        lines = stdout.readlines()
        regex = re.compile(r'Runtime\: (\d+) seconds')
        for line in lines:
            if regex.findall(line):
                runtimes.append(int(regex.findall(line)[0]))
        print(runtimes)

if __name__ == "__main__":
    sys.exit(main(sys.argv))
