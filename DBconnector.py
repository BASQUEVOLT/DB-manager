# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 21:29:30 2024

@author: EugenioCalandrini
"""

import mysql.connector
import base64
import pandas as pd
import numpy as np

class DBConnector:
    def __init__(self):
        self.host = 'testlab-bd.testlab-bd.private.mysql.database.azure.com'
        self.user = 'testlab-user'
        self.password = 'cFZ5MDA0NXBsTFJBMXVpZ1pxaVo='
        self.database = 'testlab-db'
        self.conn = None
        self.cursor = None
    
    def connect(self):
        # Connect to MySQL
        self.conn = mysql.connector.connect(
            host = self.host,
            user = self.user,
            password = base64.b64decode(self.password).decode("utf-8"),
            database = self.database
        )
        
    def set_cursor(self):
        self.cursor = self.conn.cursor()
    
    def close_connection(self):
        self.cursor.close()
        self.conn.close()
        
    def cast_variable(self, df, data_type):
        data_dict = {i:j for i,j in zip(df.columns, data_type)}
        
        for i in df.columns:
            if data_dict[i] == 'float':
                df[i] = df[i].str.replace(',', '.')
        
        df.replace('', np.nan, inplace=True)
        df = df.astype(data_dict)
        return df
        
    def fetch_records(self, id, mod=60):
        table="record"
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE barcode LIKE '%{id}%' AND record_id mod '{mod}' = 0")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'int64', 'int64', 'int64', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float'])
        
        return df
    
    def fetch_cycle(self, id):
        table="cycle"
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE barcode LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float', 'float', 'float', 'float'])
        
        return df
    
    def fetch_step(self, id):
        table="step"
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE barcode LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'int64', 'int64', 'str', 'float', 'float', 'float', 'str', 'float', 'float', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str' ])
        
        return df
    
    def fetch_status(self, id):
        table="channel_status"
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE packBarCode LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['str', 'int64', 'int64', 'int64', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'int64', 'str', 'int', 'str', 'str', 'str', 'str', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str'])
        
        return df
    
    def fetch_schedule(self, id):
        table="schedule"
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE barcode LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'str', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'float'])
        
        return df    
    
    def fetch_cell_parameters(self, id):
        table="pouch_cell_parameters"
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        
        data_dict = {
            'Cathode_batch' : 'str',
            'Vicarli_system_ID' : 'str',
            #'WEEK' : 'int',
            'Stacking_date' : 'str',
            'Assembly_date' : 'str',
            'Active_material' : 'str',
            'Number_Cathode_Layers' : 'int',
            'Cathode_Area' : 'float',
            'Total_Mass' : 'float',
            'Active_mass' : 'float',
            'Cathode_active_layer_thickness_DS' : 'float',
            'Porosity' : 'float',
            'Cathode_density' : 'float',
            'Rated_Capacity' : 'float',
            'Areal_loading' : 'float',
            'Areal_capacity' : 'float',
            'Dry_pouch_mass' : 'float',
            'Electrolyte_Gen' : 'str',
            'Plasticizer_name' : 'str',
            'Electrolyte_mass' : 'float',
            }
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE Vicarli_system_ID LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = df[data_dict.keys()]
        df = self.cast_variable(df, data_type=data_dict.values())
        
        return df
    
    def fetch_ids(self):
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        # self.cursor.execute(f"SELECT *  FROM `testlab-db`.channel_status")
        self.cursor.execute(f"SELECT packBarCode,btsSysState  FROM `testlab-db`.channel_status")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        
        return df    
    
    def fetch_formation(self, id):
        """_summary_

        Args:
            id (_type_): _description_

        Returns:
            _type_: _description_
        """
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        self.cursor.execute(f"SELECT distinct(test_id) FROM `testlab-db`.schedule where barcode like '%{id}%' and Builder like 'FM';")
        test_id = self.cursor.fetchall()[0][0]
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM `testlab-db`.record where barcode like '%{id}%' AND record_id mod 60 = 0 and test_id like '{test_id}';")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'int64', 'int64', 'int64', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float'])
        
        return df 
    
    def fetch_leakagetest(self, id):
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor()
        self.cursor.execute(f"SELECT distinct(test_id) FROM `testlab-db`.schedule where barcode like '%{id}%' and Builder like 'FM';")
        test_id = self.cursor.fetchall()[0][0]
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM `testlab-db`.record where barcode like '%{id}%' AND step_id between 34 and 37;")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'int64', 'int64', 'int64', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float'])
        
        return df 
        
        