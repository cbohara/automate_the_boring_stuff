#!/usr/bin/env python
import sys
import csv
import datetime as dt
from collections import defaultdict
import configargparse
import boto3


def ensure_trailing_slash(path):
    if path.endswith("/"):
        return path
    else:
        return path+"/"


def last_day_of_previous_month(YYYYMM):
    first_day_of_current_month = dt.datetime.strptime(YYYYMM, "%Y%m")
    one_day_delta = dt.timedelta(days=1)
    last_day_of_last_month = first_day_of_current_month - one_day_delta
    return last_day_of_last_month.strftime("%Y-%m-%d")


def date_plus_index(default_date, index):
    default_date = dt.datetime.strptime(default_date, "%Y-%m-%d")
    day_delta = dt.timedelta(days=index)
    adjusted_observe_date = default_date + day_delta
    return adjusted_observe_date.strftime("%Y-%m-%d")


def main(args):
    """
    script to submit Verve Activate audience to SQS queue

    requirements:
    - awscli
    - specify region in ~/.aws/config
    [default]
    region=us-east-1
    - download excel file as .csv
    - specify refresh month as YYYYMM

    ex: python csv_to_sqs_msg.py file.csv YYYYMM
    """
    # parse command line arguments
    p = configargparse.ArgParser()
    p.add('queue', help='specify SQS to add messages to')
    p.add('csvfile', help='csvfile containing audience data')
    p.add('YYYYMM', help='specify YYYYMM for refresh month')
    args = p.parse_args()
    queue = args.queue
    csvfile = args.csvfile
    YYYYMM = args.YYYYMM

    if not csvfile.endswith('.csv'):
        raise Exception('This script will only work with .csv files, not with .xlsx files')

    # d = {'id1': ['s3://path1', 's3://path2'], 'id2': ['s3://path3']}
    d = defaultdict(list)

    # get data from csv file
    with open(csvfile) as f:
        csv_data = csv.reader(f)

        for index, row in enumerate(csv_data):
            # ignore header
            if index == 0:
                continue

            id = row[1].strip()
            s3path = row[3].strip()

            # there may be multiple s3 paths within a single field in the csv vlsm column so add each path separately
            if "," in s3path:
                s3paths = s3path.split(",")
                for s3path in s3paths:
                    s3path = ensure_trailing_slash(s3path.strip())
                    d[id].append(s3path)
                continue

            # otherwise add single path to dictionary
            s3path = ensure_trailing_slash(s3path)
            d[id].append(s3path)

    # current practice is to lable the observe date as the last date of the previous month
    default_observe_date = last_day_of_previous_month(YYYYMM)

    # generate list of msgs to be passed to sqs
    msgs = []
    for item in d.items():
        id = item[0]
        s3paths = item[1]

        # vlsm uploads to the same id cannot have the same observe date
        if len(s3paths) > 1:
            for index, path in enumerate(s3paths):
                # increment the date by 1 to avoid vlsm upload conflict
                adjusted_observe_date = date_plus_index(default_observe_date, index)
                msgs.append(id+"|"+adjusted_observe_date+"|"+path)
        else:
            # otherwise use the default observe date
            msgs.append(id+"|"+default_observe_date+"|"+s3paths[0])

    # use boto3 to connect to sqs queue
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue)

    # push each message to sqs 
    for msg in msgs:
        response = queue.send_message(MessageBody=msg)
        print "Pushing message:"
        print msg
        print "SQS response:"
        print response


if __name__ == "__main__":
    main(sys.argv)
