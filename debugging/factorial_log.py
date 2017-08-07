#!/usr/bin/env python3
import logging

def main():
    logging.basicConfig(filename="logger.txt", level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.disable(logging.DEBUG)
    logging.debug('Start of program')

    def factorial(n):
        logging.debug('Start of factorial {}'.format(n))
        total = 1
        for i in range(1, n + 1):
            total *= i
            logging.debug('i is {}, total is {}'.format(i, total))
        return total
        logging.debug('End of factorial {}'.format(n))

    print(factorial(5))
    logging.debug('End of program')


if __name__ == '__main__':
    main()
