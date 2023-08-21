import boto3
from datetime import datetime
from datetime import timedelta
from datetime import date

CURR_DATE = date.today().strftime('%Y-%m-%d') # date formatted as a string
PREV_DATE = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')  # date formatted as a string
FULLLOAD_DATE = (date.today() - timedelta(days = 5)).strftime('%Y-%m-%d')  # date formatted as a string
SEARCH_TEXT = "<REPLACEDATE>"

s3 = boto3.resource('s3')
BUCKET = "crawlerpathena"


def replace_text(v_filenm,v_new_filenm,v_search_text,v_replace_text):
    with open(v_filenm, 'r') as file:
        data = file.read()
        data = data.replace(v_search_text, v_replace_text)

    with open(v_new_filenm, 'w') as file:
        file.write(data)    
    
    return True



def main():

    # 
    replace_text("data-template/cdcload.csv","data/cdcload.csv",SEARCH_TEXT,PREV_DATE)
    replace_text("data-template/cdcload-nextpartition.csv","data/cdcload-nextpartition.csv",SEARCH_TEXT,CURR_DATE)
    replace_text("data-template/fullload.csv","data/fullload.csv",SEARCH_TEXT,FULLLOAD_DATE)
    

    #create bucket
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={ 'LocationConstraint': 'us-west-2'})

    # fullload
    s3.Bucket(BUCKET).upload_file("data/fullload.csv", "fullload/fullload.csv")

    # cdcload
    s3.Bucket(BUCKET).upload_file("data/cdcload-nextpartition.csv", "cdcload/" + CURR_DATE +"/cdcload.csv")
    s3.Bucket(BUCKET).upload_file("data/cdcload.csv", "cdcload/" + PREV_DATE +"/cdcload.csv")
    
    for bucket in s3.buckets.all():
      print(bucket.name)

if __name__ == "__main__":
    main()
