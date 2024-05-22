# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 21:29:30 2024

@author: EugenioCalandrini
"""

import mysql.connector
import base64
import pandas as pd
import numpy as np

class DBconnector:
    """
    This is a class to manage the access to the database data.
    """
    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()
    
    def connect(self):
        # Connect to MySQL
        self.conn = mysql.connector.connect(
            host = self.host,
            user = self.user,
            password = base64.b64decode(self.password).decode("utf-8"),
            database = self.database
        )
        
    def set_cursor(self):
        # Create a cursor object
        self.cursor = self.conn.cursor()
    
    def close_connection(self):
        # Close cursor and connection
        self.cursor.close()
        self.conn.close()
        
    def cast_variable(self, df, data_type):
        """
        This function makes some changes in df dataframe to prepare data for analysis

        Args:
            df (pandas dataframe): data
            data_type (list): list with the datatype corresponding to df columns

        Returns:
            df (pandas dataframe): with float columns replacing ',' for '.' and the empty data for np.nan
        """
        data_dict = {i:j for i,j in zip(df.columns, data_type)}
        
        for i in df.columns:
            if data_dict[i] == 'float':
                df[i] = df[i].str.replace(',', '.')
        
        df.replace('', np.nan, inplace=True)
        df = df.astype(data_dict)
        return df
    
    def fetch_records(self, id):
        """This is the function that gets the data from the database. It access to the 'record' table.

        Args:
            id (str): Cell ID from Vicarli System.
            mod (int, optional): Each <mod> data you are going to pick one. Defaults to 60.

        Returns:
            df(pandas dataframe): dataframe with neware data about the requested cell
        """
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT cycle_id, step_type, record_time, voltage, current, capacity, TotalTime FROM `testlab-db`.record where barcode = '{id}';")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'str', 'float', 'float', 'float', 'float', 'float'])
        
        return df
    
    def fetch_cycle(self, id):
        """This is the function that gets the data from the database. It access to the 'cycle' table of 'dev-db' database.

        Args:
            id (str): Cell ID from Vicarli System.

        Returns:
            df(pandas dataframe): dataframe with neware data about the requested cell
        """
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM `dev-db`.`cycle_data` WHERE barcode = '{id}'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        
        return df
    
    def fetch_step(self, id):
        """This is the function that gets the data from the database. It access to the 'step' table.

        Args:
            id (str): Cell ID from Vicarli System.

        Returns:
            df(pandas dataframe): dataframe with neware data about the requested cell
        """
        table="step"
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE barcode LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'int64', 'int64', 'str', 'float', 'float', 'float', 'str', 'float', 'float', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str' ])
        
        return df
    
    def fetch_status(self, id):
        """This is the function that gets the data from the database. It access to the 'channel_status' table.

        Args:
            id (str): Cell ID from Vicarli System.

        Returns:
            df(pandas dataframe): dataframe with neware data about the requested cell
        """
        table="channel_status"
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE packBarCode LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['str', 'int64', 'int64', 'int64', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'int64', 'str', 'int', 'str', 'str', 'str', 'str', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str'])
        
        return df
    
    def fetch_schedule(self, id):
        """This is the function that gets the data from the database. It access to the 'schedule' table.

        Args:
            id (str): Cell ID from Vicarli System.

        Returns:
            df(pandas dataframe): dataframe with neware data about the requested cell
        """
        table="schedule"
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM {table} WHERE barcode LIKE '%{id}%'")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'str', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'float'])
        
        return df    
    
    def fetch_cell_parameters(self, id):
        """This is the function that gets the data from the database. It access to the 'pouch_cell_parameters' table.
        In this case, we select some of the columns available in the database, not all

        Args:
            id (str): Cell ID from Vicarli System.

        Returns:
            df(pandas dataframe): dataframe with neware data about the requested cell
        """
        table="pouch_cell_parameters"
                
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
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = df[data_dict.keys()]
        df = self.cast_variable(df, data_type=data_dict.values())
        
        return df
    
    def fetch_ids(self):
        """This is a function to get all the ids from the database.

        Returns:
            df(pandas dataframe): dataframe with ids. 
        """
                
        # Create a cursor object
        self.set_cursor()
        
        # Fetch all records from the table
        # self.cursor.execute(f"SELECT *  FROM `testlab-db`.channel_status")
        self.cursor.execute(f"SELECT packBarCode,btsSysState  FROM `testlab-db`.channel_status") #We get the columns: packBarCode and btsSysState
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        
        return df    
    
    def fetch_formation(self, id):
        """
        Function to download the formation data of a single cell from the Neware Database. data is taken mod 60

        Parameters
        ----------
        id : string
            Cell ID from Vicarli System.

        Returns
        -------
        df : pd dataframe
            data from the record table.

        """

        # Create a cursor object
        self.set_cursor()
        self.cursor.execute(f"SELECT distinct(test_id) FROM `testlab-db`.schedule where barcode like '%{id}%' and Builder like 'FM';")
        test_id = self.cursor.fetchall()[0][0] #Here we get the code for the formation protocol of the given cell
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM `testlab-db`.record where barcode like '%{id}%' AND record_id mod 60 = 0 and test_id like '{test_id}';")
        records = self.cursor.fetchall()  #And here finally we get the data 
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'int64', 'int64', 'int64', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float'])
        
        return df 
    
    def fetch_leakagetest(self, id):
        """
        Function to download the leakage test data of a single cell from the Neware Database.

        Parameters
        ----------
        id : string
            Cell ID from Vicarli System.

        Returns
        -------
        df : pd dataframe
            data from the record table.

        """
                
        # Create a cursor object
        self.set_cursor()
        self.cursor.execute(f"SELECT distinct(test_id) FROM `testlab-db`.schedule where barcode like '%{id}%' and Builder like 'FM';")
        test_id = self.cursor.fetchall()[0][0]
        
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM `testlab-db`.record where barcode like '%{id}%' AND step_id between 34 and 37;")
        records = self.cursor.fetchall()#Here we get the code for the formation protocol of the given cell
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'str', 'str', 'str', 'int64', 'int64', 'int64', 'int64', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'str', 'str', 'str', 'str', 'float', 'float'])
        
        return df 
    
    def general_query(self, query_string):
                        
        # Create a cursor object
        # self.set_cursor()
        
        # Fetch all records from the table
        self.cursor.execute(query_string)
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        
        return df 
    
    def fetch_testid(self, id):
                
        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        self.cursor.execute(f"SELECT DISTINCT test_id, Builder FROM `testlab-db`.schedule WHERE barcode LIKE '%{id}%';")
        records = self.cursor.fetchall()
               
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'str'])
        
        return df 
    
    def fetch_activemass(self, id):
                
        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        self.cursor.execute(f"SELECT DISTINCT active_material FROM `testlab-db`.schedule WHERE barcode LIKE '%{id}%';")
        active_mass = float(self.cursor.fetchall()[0][0].replace(",", "."))
               
        return active_mass
    
    def fetch_status(self, id):

        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        self.cursor.execute(f"SELECT btsSysState FROM `testlab-db`.channel_status WHERE packBarCode LIKE '%{id}%';")
        record = self.cursor.fetchall()[0][0]
               
        return record
    
    def fetch_chamber(self, id):
                
        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        # self.cursor.execute(f"SELECT CONCAT(equiptCode, ",", channelNo, ",", channelCode) FROM `testlab-db`.channel_status WHERE packBarCode LIKE '%{id}%';")
        self.cursor.execute(f"SELECT DISTINCT Builder, chl_id FROM `testlab-db`.schedule WHERE barcode LIKE '%{id}%';")
        records = self.cursor.fetchall()
        
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
               
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['str', 'str'])
        
        return df 
    
    def fetch_lastCycleNumber(self, id):
                
        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        self.cursor.execute(f"SELECT cycle_id from `testlab-db`.cycle where barcode like '%{id}%' ORDER BY CAST(cycle_id AS UNSIGNED) DESC LIMIT 1;")
        record = int(self.cursor.fetchall()[0][0])
               
        return record
    
    def fetch_protocol(self, id):
                
        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        self.cursor.execute(f"SELECT test_id, seq_id, step_id, step_type, step_time, setting_voltage, setting_rate, setting_current, cut_of_rate, cut_of_current, recording_conditions FROM `testlab-db`.schedule WHERE barcode LIKE '%{id}%';")
        records = self.cursor.fetchall()
               
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['int64', 'int64', 'int64', 'str', 'str', 'float', 'float', 'float', 'float', 'float', 'str'])
        
        return df 
    
    def fetch_time(self, id):
                
        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        self.cursor.execute(f"SELECT DISTINCT Builder, StartTIme, EndTime FROM `testlab-db`.schedule WHERE barcode LIKE '%{id}%';")
        records = self.cursor.fetchall()
               
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df = self.cast_variable(df, data_type=['str', 'str', 'str'])
        
        return df
    
    def fetch_qctable(self,):
                
        # Create a cursor object
        self.set_cursor()
        # Fetch all records from the table
        self.cursor.execute(f"SELECT * FROM `lab-db`.all_merged;")
        records = self.cursor.fetchall()
               
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        data_dict = {
            'number':int,                                   
            'Vicarli_system_ID':str,                 
            'Cathode_type':str,                              
            'Cathode_material':str,                          
            'Anode_Type':str,                                
            'Electrolyte_Gen':str,                            
            'Number_Cathode_Layers':int,                     
            'Cathode_Area':float,                            
            'Active_mass':float,                             
            'Rated_Capacity':float,                          
            'IR_BWT':float,                                  
            'IR_AWT':float,                                  
            'IR_AXL':float,                                  
            'IR_AFM':float,                                  
            'IR_ADG':float,                                  
            'IR_EOL':float,                                 
            'OCV_BWT':float,                                 
            'OCV_AWT':float,                                
            'OCV_AXL':float,                                 
            'OCV_AFM':float,                                 
            'OCV_ADG':float,                                 
            'OVC_EOL':float,                                 
            'Date_BWT':str,                                  
            'Date_AWT':str,                                  
            'Date_AXL':str,                                 
            'Date_AFM':str,                                  
            'Date_ADG':str,                                 
            'Date_EOL':str,                                 
            'Cell_ID_Vicarli':str,                          
            'Grindometer':str,                              
            'Rheology':str,                                 
            'Solid_content':str,                           
            'Moisture_after_drying':float,                    
            'Interfacial_resistance':float,                   
            'Composite_volume_resistance':float,              
            'Average_electrode_thickness':float,              
            'Porosity':str,                                 
            'Mass_loading':float,                              
            'Visual_check':str,                              
            'Separator_moisture_content':float,               
            'Electrolyte_batch_number':str,                 
            'Elecetrolyte_moisture_content':str,            
            'Electrolyte_chemical_composition':str,         
            'Thickness_of_stack':float,                       
            'Mass_of_final_stack':float,                      
            'Alignment_of_electrodes':str,                  
            'Alignment_separator':str,                      
            'Date_of_stacking':str,                          
            'Insulation_test':str,                          
            'Resistance_of_stack':str,                      
            'Resistance_of_stack_and_tabs_Anode':float,       
            'Insulation_test_2':str,                        
            'Resistance_of_stack_and_tabs_cathode':float,     
            'Visual_inspection':str,                        
            'Mass_of_pouch_and_stack':float,                   
            'Visual_check_of_pouch':str,                    
            'Insulation_test_3':str,                        
            'Mass_of_stack':float,                            
            'Mass_of_electrolyte':float,                      
            'Density_of_electrolyte':float,                   
            'Date_of_filling':float,                          
            'OCV':float,                                      
            'ACIR':float, 
            'max_cycle':float,                               
            'total_specific_discharge_energy':float,               
            'specific_discharge_energy_avg':float,                 
            }
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)

        df = df.astype(data_dict)


        return df 
       
    def fetch_cell_info(self, id):
        """
        Function to fetch all the info of the cell with the given id.

        Parameters
        ----------
        id : str
            cell id, e.g. '1003-BQV000000000000217-1'.

        Returns
        -------
        df : pandas dataframe
            dataframe with the cell information.

        """
        df = self.general_query(f"SELECT * FROM `dev-db`.ids WHERE barcode_corrected = '{id}';")
        return df
    
    def fetch_specific_capacities(self, id):
        df = self.general_query(f"SELECT cycle_id, specific_chg_capa, specific_dchg_capa FROM `dev-db`.cycle_data WHERE barcode LIKE '{id}';")
        return df
    
    def fetch_specific_efficiencies(self, id):
        df = self.general_query(f"SELECT cycle_id, ce, rte, my_soh FROM `dev-db`.cycle_data WHERE barcode LIKE '{id}';")
        return df
    
    def fetch_steptime(self, id):
        df = self.general_query(f"SELECT cycle_id, step_type, step_time FROM `dev-db`.step_time WHERE barcode = '{id}';")
        pivot_df = pd.pivot(df, values='step_time', index='cycle_id', columns='step_type').reset_index()
        return pivot_df
    
    def fetch_end_voltage(self, id):
        df = self.general_query(f"SELECT cycle_id, step_type, previous_step_type, end_voltage FROM `dev-db`.end_voltage WHERE barcode = '{id}';")
        pivot_df = pd.pivot(df, values='end_voltage', index='cycle_id', columns='previous_step_type').reset_index()
        return pivot_df
    
    def fetch_hioki(self):
        # Connect to MySQL
        self.connect()
                
        # Create a cursor object
        self.set_cursor(prepared=True)
        # Fetch all records from the table
        self.cursor.execute("SELECT * FROM `testlab-db`.hioki_data;")
        records = self.cursor.fetchall()
               
        # Get column names
        column_names = [i[0] for i in self.cursor.description]
        
        # Close cursor and connection
        #self.close_connection()
        
        # Load records into Pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)
        df_hioki_decoded = df.map(self.decode_bytes)
        column_types = {
            'ID': str,
            'Start_time': str,
            'Completion_time': str,
            'Email':str,
            'Name':str,
            'Last_modified_time':str,
            'Date':str,
            'State':str,
            'OCV':float,
            'IR':float,
            'Cell_Barcode':str,
            'OCV_ok':str,
            'IR_ok':str
            }
        df_hioki = df_hioki_decoded.astype(column_types)
        return df_hioki
    
    def decode_bytes(self, value):
        if isinstance(value, bytearray):
            return value.decode()
        else:
            return value

        
        