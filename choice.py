import athenamodule
import time
from datetime import timedelta
from datetime import date


CURR_DATE = date.today().strftime('%Y-%m-%d') # date formatted as a string
PREV_DATE = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')  # date formatted as a string
FULLLOAD_DATE = (date.today() - timedelta(days = 5)).strftime('%Y-%m-%d')  # date formatted as a string
SEARCH_TEXT = "<REPLACEDATE>"



# Implement Python Switch Case Statement using Dictionary

def one():
    execution_id_1 = athenamodule.create_rawdatabase()
    print(f"Checking query execution for: {execution_id_1}")
    execution_id_1 = athenamodule.create_rawstddatabase()
    print(f"Checking query execution for: {execution_id_1}")
    return True

def two():
    execution_id_2 = athenamodule.create_table("queries/fullload.ddl")
    print(f"Create Table execution id: {execution_id_2}")
    return True

def three():
    execution_id_3 = athenamodule.create_table("queries/incremental.ddl")
    print(f"Create Table execution id: {execution_id_3}")
    return True

def four():
    execution_id_4 = athenamodule.create_table("queries/iceberg.ddl")
    print(f"Create Table execution id: {execution_id_4}")
    return True

def five():
    athenamodule.replace_text("query-template/load-partitions.sql","queries/load-partitions.sql",SEARCH_TEXT,PREV_DATE)
    execution_id_5 = athenamodule.run_query("queries/load-partitions.sql")
    print(f"Add partitions execution id: {execution_id_5}")
    return True

def six():
    execution_id_6 = athenamodule.run_query("queries/one-off.sql")
    print(f"Query execution id: {execution_id_6}")
    return True

def seven():
    athenamodule.replace_text("query-template/merge.sql","queries/merge.sql",SEARCH_TEXT,PREV_DATE)
    execution_id_7 = athenamodule.run_query("queries/merge.sql")
    print(f"Query execution id: {execution_id_7}")
    return True

def eight():
    execution_id_7 = athenamodule.run_query("queries/vaccum.sql")
    print(f"Query execution id: {execution_id_7}")
    return True

def default():
    return "Incorrect option selected"

def switch(x):
    switcher = {
        "1": one,
        "2": two,
        "3": three,
        "4": four,
        "5": five,
        "6": six,
        "7": seven,
        "8": eight
        
    }
    return switcher.get(x, default)()


   

options = ['create database', 
           'create fulload table', 
           'create intraday', 
           'create iceberg table', 
           'add intraday partitions', 
           'one-off load', 
           'merge', 
           'vaccum',
           'EXIT']

user_input = ''

input_message = "Pick an option:\n"

for index, item in enumerate(options):
    input_message += f'{index+1}) {item}\n'

input_message += 'Your choice: '

while True:
    user_input = input("HELLO " + input_message)

    print(f"USER INPUT IS: {user_input}")

    if user_input == '9':
        print("Exiting....")
        break
    else:
        print(switch(user_input))
        continue
    
    