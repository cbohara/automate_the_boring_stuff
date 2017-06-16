#!/usr/local/bin/python3
import logging
import mylib


def main():
    logging.basicConfig(filename='myapp.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO)
    logging.info('Started')
    mylib.do_something()
    logging.info('Finished')


if __name__ == '__main__':
    main()
