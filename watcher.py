import os, subprocess
from datetime import datetime, time, timedelta
import boto
import botocore

hdfs_url1 = '/data/*'
hdfs_url='/Users/vsubr2/Projects/s3hdfschecker'
hdfs = subprocess.check_output('ls -la ' + hdfs_url , shell=True)
#hdfs1 = subprocess.check_output('hadoop fs -ls ' + hdfs_url +'''| awk '{timestamp= $8 $6  "  "  $7;print timestamp}' | tail -1'''  , shell=True)

def s3watch():
    s3 = boto.connect_s3()
    #exists = False
    print s3
    bucket_name = 'inbound-adhoc'
    bucket = s3.get_bucket(bucket_name)
    # go through the list of files
    bucket_list = bucket.list()
    for l in bucket_list:
        keyString = str(l.key)
        if keyString =='test.txt':
            print 'It exists ' + keyString
    return True


def hdfswatcher():
    todaydate = datetime.now().date()
    todaytime = datetime.now()
    print "Today's date " + str(todaydate) + " \nToday's time is " + str(todaytime)
    # yesterday = today - datetime.timedelta(days=2)

    # lastactive = hdfs1
    lastactive = '2016-03-17  18:49'
    filedate = lastactive[0:10]
    filetime = lastactive[12:17]
    combine = datetime.combine(datetime.strptime(filedate, "%Y-%m-%d"), datetime.strptime(filetime, "%H:%M").time())
    print '\nDate of the latest file ' + filedate + ' \nTime of the latest file ' + filetime
    if str(filedate) == str(todaydate):
        print 'Matches'
        return True
    else:
        print 'Does not match'


if __name__ == '__main__':
    if s3watch() is True and hdfswatcher() is True:
        print("True")
    else:
        print("False")

