
''' athenamodule.py '''


import boto3
import time
from datetime import timedelta
from datetime import date


CLIENT = boto3.client("athena")

DATABASE_NAME = "demo"
RAW_DATABASE_NAME = "raw"
RAWSTD_DATABASE_NAME = "rawstd"
RESULT_OUTPUT_LOCATION = "s3://crawlerpathena/query-results/"

TABLE_NAME = "funding_data"
CURR_DATE = date.today().strftime('%Y-%m-%d') # date formatted as a string
PREV_DATE = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')  # date formatted as a string
FULLLOAD_DATE = (date.today() - timedelta(days = 5)).strftime('%Y-%m-%d')  # date formatted as a string
SEARCH_TEXT = "<REPLACEDATE>"




def replace_text(v_filenm,v_new_filenm,v_search_text,v_replace_text):
    with open(v_filenm, 'r') as file:
        data = file.read()
        data = data.replace(v_search_text, v_replace_text)

    with open(v_new_filenm, 'w') as file:
        file.write(data)    
    
    return True


def has_query_succeeded(execution_id):
    state = "RUNNING"
    max_execution = 5

    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution -= 1
        response = CLIENT.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return True

        time.sleep(30)

    return False


def create_rawdatabase():
    response = CLIENT.start_query_execution(
        QueryString=f"create database {RAW_DATABASE_NAME}",
        ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
    )

    return response["QueryExecutionId"]

def create_rawstddatabase():
    response = CLIENT.start_query_execution(
        QueryString=f"create database {RAWSTD_DATABASE_NAME}",
        ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
    )

    return response["QueryExecutionId"]


def create_table(createQuery: str):
    with open(createQuery) as ddl:
        response = CLIENT.start_query_execution(
            QueryString=ddl.read(),
            ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
        )

        return response["QueryExecutionId"]
    


def run_query(runQuery: str):
    with open(runQuery) as sql:
        response = CLIENT.start_query_execution(
            QueryString=sql.read(),
            ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
        )

        return response["QueryExecutionId"]    


def get_num_rows():
    query = f"SELECT COUNT(*) from {DATABASE_NAME}.{TABLE_NAME}"
    response = CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
    )

    return response["QueryExecutionId"]


def get_query_results(execution_id):
    response = CLIENT.get_query_results(
        QueryExecutionId=execution_id
    )

    results = response['ResultSet']['Rows']
    return results