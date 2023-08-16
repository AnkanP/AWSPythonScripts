import boto3
import time

CLIENT = boto3.client("athena")

DATABASE_NAME = "demo"
RESULT_OUTPUT_LOCATION = "s3://crawlerpathena/query-results/"

TABLE_NAME = "funding_data"


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


def create_database():
    response = CLIENT.start_query_execution(
        QueryString=f"create database {DATABASE_NAME}",
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


def main():
    # 1. Create Database
    execution_id_1 = create_database()
    print(f"Checking query execution for: {execution_id_1}")

    # 2. Create fulload Table, incremental & iceberg table
  #  execution_id_2 = create_table("queries/fullload.ddl")
  #  print(f"Create Table execution id: {execution_id_2}")
#
  #  execution_id_3 = create_table("queries/incremental.ddl")
  #  print(f"Create Table execution id: {execution_id_3}")
#
  #  execution_id_4 = create_table("queries/iceberg.ddl")
  #  print(f"Create Table execution id: {execution_id_4}")
#
  #  # 3. Add partitions
  #  execution_id_5 = run_query("queries/load-partitions.sql")
  #  print(f"Add partitions execution id: {execution_id_5}")
#
  #  # 4. One-off load into iceberg
  #  execution_id_6 = run_query("queries/one-off.sql")
  #  print(f"Query execution id: {execution_id_6}")
#
  #  # 5. Merge cdc load into iceberg
  #  execution_id_7 = run_query("queries/merge.sql")
  #  print(f"Query execution id: {execution_id_7}")

# 5. Merge cdc load into iceberg
    execution_id_7 = run_query("queries/vaccum.sql")
    print(f"Query execution id: {execution_id_7}")

    # 2. Check query execution
   # query_status = has_query_succeeded(execution_id=execution_id)
   # print(f"Query state: {query_status}")

    # 3. Create Table
    #execution_id = create_table()
    #print(f"Create Table execution id: {execution_id}")

    # 4. Check query execution
    #query_status = has_query_succeeded(execution_id=execution_id)
    #print(f"Query state: {query_status}")

    # 5. Query Athena table
    #execution_id = get_num_rows()
    #print(f"Get Num Rows execution id: {execution_id}")

    #query_status = has_query_succeeded(execution_id=execution_id)
    #print(f"Query state: {query_status}")

    # 6. Query Results
    #print(get_query_results(execution_id=execution_id))


if __name__ == "__main__":
    main()