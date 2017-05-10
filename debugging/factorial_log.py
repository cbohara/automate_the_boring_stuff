import sys
import logging


def factorial(n):
    """Return factorial value of n input."""
    logging.debug("Start of factorial(%s)" % (n))
    total = 1
    for i in range(n + 1):
        total *= i
        logging.debug("i is " + str(i) + ", total is " + str(total))
    logging.debug("End of factorial(%s)" % (n))
    return total

def main(script):
    """Basic example of using logging module."""
    logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(messages)s')
    logging.debug("Start of program")

    print(factorial(5))
    logging.debug("End of program")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
