import sys


def collatz(number):
    if number % 2 == 0:
        return number // 2
    else:
        return 3 * number + 1


def main(script):
    try:
        argv = sys.argv[1]
    except IndexError:
        print('python3 collatz.py [number]')
    else:
        try:
            number = int(argv)
        except ValueError:
            print('Please input integer value')
        else:
            print(collatz(number))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
