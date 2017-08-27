#!/usr/bin/env python
import sys
import threading
import boto3


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._seen_so_far = 0
        self._lock = threading.Lock()
    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            sys.stdout.write("\r\n%s --> %s bytes transferred\n" % (self._filename,self._seen_so_far))
            sys.stdout.flush()


def main():
    """Transfer key-values from one s3 location to another s3 location"""
    s3 = boto3.client('s3')

    copy_source = {
        'Bucket': 'source-bucket-name',
        'Key': 'path/to/filename.txt'
    }

    s3.copy(copy_source, 'destination-bucket', 'path/to/destination/filename.txt',
            Callback=ProgressPercentage('destination-bucket/path/to/destination/filename.txt'))


if __name__ == "__main__":
    main()
