#!/usr/bin/env python
import sys
import csv
import datetime as dt
from subprocess import check_call
from collections import defaultdict
import boto3


def ensure_trailing_slash(path):
    if path.endswith('/'):
        return path
    else:
        return path+'/'


def last_day_of_previous_month(YYYYMM):
    first_day_of_current_month = dt.datetime.strptime(YYYYMM, '%Y%m')
    one_day_delta = dt.timedelta(days=1)
    last_day_of_last_month = first_day_of_current_month - one_day_delta
    return last_day_of_last_month.strftime('%Y-%m-%d')


def date_plus_index(default_date, index):
    default_date = dt.datetime.strptime(default_date, '%Y-%m-%d')
    day_delta = dt.timedelta(days=index)
    adjusted_observe_date = default_date + day_delta
    return adjusted_observe_date.strftime('%Y-%m-%d')


def main(args):
    """
    script to submit data from csv as message to SQS queue

    requirements:
    - awscli
    - specify region in ~/.aws/config
    [default]
    region=us-east-1
    - download excel file as .csv
    - specify refresh month as YYYYMM
    """
    try:
        queue = sys.argv[1]
        csvfile = sys.argv[2]
        YYYYMM = sys.argv[3]
    except IndexError:
        print "python va_sqs.py queue_name csvfile.csv YYYYMM"
    else:
        # dictionary will have id as key and list of s3paths as value
        d = defaultdict(list)

        # get data from csv file
        with open(csvfile) as f:
            csv_data = csv.reader(f)

            for index, row in enumerate(csv_data):
                # ignore csv header
                if index == 0:
                    continue

                id = row[1].strip()
                s3path = row[3].strip()

                # there may be multiple s3 paths within a single field in the csv column so add each path separately
                if ',' in s3path:
                    s3paths = s3path.split(',')
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
        for key in d.items():
            id = key[0]
            s3paths = key[1]
            #  uploads to the same id cannot have the same observe date
            if len(s3paths) > 1:
                for index, path in enumerate(s3paths):
                    # increment the date by 1 to avoid  upload conflict
                    adjusted_observe_date = date_plus_index(default_observe_date, index)
                    msgs.append(id+"|"+adjusted_observe_date+"|"+path)
            else:
                # otherwise use the default observe date
                msgs.append(id+"|"+default_observe_date+"|"+s3paths[0])

        # use boto3 to connect to sqs queue
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue)

        # push each message to sqs 
        for msg in msgs:
            pass
#            response = queue.send_message(MessageBody=msg)
#            msgID = response.get('MessageID')
#            md5 = reponse.get('MD5OfMessageBody')
            print msg
#            print "successfully added to queue with MessageId " + msgID + " and MD5OfMessageBody " + md5


if __name__ == "__main__":
    main(sys.argv)
