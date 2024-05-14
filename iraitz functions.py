import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

host=os.getenv('NEWARE_SQL_HOST')
user=os.getenv('NEWARE_SQL_USER')
password=os.getenv('NEWARE_SQL_PASSWORD')
database = 'testlab-db'




def get_id(host, user, password, database):
    # Connect to MySQL
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Fetch all records from the table
    cursor.execute(f"SELECT DISTINCT(barcode) FROM cycle ORDER BY 'batch_no';")
    records = cursor.fetchall()
    
    # Get column names
    column_names = [i[0] for i in cursor.description]
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    
    # Load records into Pandas DataFrame
    df = pd.DataFrame(records, columns=column_names)
    
    return df

def fetch_records_into_dataframe(host, user, password, database, table, id):
    
    # Connect to MySQL
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    # Create a cursor object
    if table=='hioki_data':
        cursor = conn.cursor(prepared=True)
    else:
        cursor = conn.cursor()
    
    # Fetch all records from the table
    if table in ['pouch_cell_parameters','hioki_data', 'quality_measures']:
        cursor.execute(f"SELECT * FROM {table}")
    else:
        cursor.execute(f"SELECT * FROM {table} WHERE barcode LIKE '{id}'")
    
    records = cursor.fetchall()
    # Get column names
    column_names = [i[0] for i in cursor.description]
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    
    # Load records into Pandas DataFrame
    df = pd.DataFrame(records, columns=column_names)
    
    return df

#My funtions
def export_database(data):
    # User parameters
    # table = 'record' # channel_status, cycle, log, schedule, step
    if 'celda' in list(data.keys()):
        id = data['celda']
        table = 'record'
    elif 'table' in list(data.keys()):
        table = data['table']
        if data['table'] in (['pouch_cell_parameters','hioki_data', 'quality_measures']):
            id=None
        else:
            raise Exception('I didint expect this table name')

    # Fetch records into DataFrame
    # df_id = get_id(host, user, password, database)
    df_data = fetch_records_into_dataframe(host, user, password, database, table, id)

    return df_data

def get_specific_data(command):
    """_summary_

    Args:
        command (str): The command to execute in the DB

    Returns:
        df(pd DataFrame): pandas dataframe with the information
    """
    df_id = get_id(host, user, password, database)

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Fetch all records from the table

    # cursor.execute(command.format(**variables))
    cursor.execute(command, multi=True)

    records = cursor.fetchall()
    
    # Get column names
    column_names = [i[0] for i in cursor.description]
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    
    # Load records into Pandas DataFrame
    df = pd.DataFrame(records, columns=column_names)
    
    return df

def get_specific_data_complex(command):
    """This function does the same as the previous one, but is used when the command is complex. more than one command in the same string.

    Args:
        command (str): The command to execute in the DB

    Returns:
        df(pd DataFrame): pandas dataframe with the information
    """
    df_id = get_id(host, user, password, database)

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Fetch all records from the table

    # cursor.execute(command.format(**variables))
    for com in command:
        cursor.execute(com)
        if cursor.with_rows:
            records = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
        else:
            pass

    # If you expect results, fetch them
    '''   if cursor.with_rows:
        records = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df = pd.DataFrame(records, columns=column_names)
    else:
        df = None
    '''
    # Close cursor and connection
    cursor.close()
    conn.close()
    
    # Load records into Pandas DataFrame
    df = pd.DataFrame(records, columns=column_names)
    
    return df