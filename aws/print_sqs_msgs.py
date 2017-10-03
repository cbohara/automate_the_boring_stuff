#!/usr/bin/env python
import sys
import boto3


def main(args):
    try:
        queue = sys.argv[1]
    except IndexError:
        print 'python print_sqs_msgs.py queue_name'
    else:
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue)

        for msg in queue.receive_messages():
            print msg.body


if __name__ == "__main__":
    sys.exit(main(sys.argv))
