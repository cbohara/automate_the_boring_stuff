import sys
import csv
import random


def main(args):
    """Read in csv file, grab student names, and generate random student names based on input number."""
    try:
        number_of_winners = int(sys.argv[1])
    except IndexError:
        print('python3 name_generator.py [number of winners]')
    else:
        with open('/Users/cbohara/code/automate/data/dg2017.csv', 'r') as f:
            f_reader = csv.reader(f)
            # create list of students
            students = [line[3] for line in f_reader]
            # delete the header
            del students[0]

            # choose number of winners based on the input number
            for i in range(number_of_winners + 1):
                # find current winner
                current = random.choice(students)
                # print current winner
                print(current)
                # remove current student from the list to avoid repeats
                students.remove(current)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
