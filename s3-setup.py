import boto3

s3 = boto3.resource('s3')
BUCKET = "crawlerpathena"


for bucket in s3.buckets.all():
    print(bucket.name)

#create bucket
s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={ 'LocationConstraint': 'us-west-2'})

# fullload
s3.Bucket(BUCKET).upload_file("/home/crawlerp/aws_projects/AWSPythonScripts/data/fullload.csv", "fullload/fullload.csv")

# cdcload
s3.Bucket(BUCKET).upload_file("/home/crawlerp/aws_projects/AWSPythonScripts/data/cdcload.csv", "cdcload/2023-08-04/cdcload.csv")