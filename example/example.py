# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 12:48:06 2024

@author: EugenioCalandrini
"""

import mysql.connector
import base64
from DBconnector import DBConnector

conn = mysql.connector.connect(
    host = 'testlab-bd.testlab-bd.private.mysql.database.azure.com',
    user = "ecalandrini",
    password = base64.b64decode("ZjNiNFo2O0ZNPDdyem5qIw==").decode("utf-8"),
    database = 'testlab-db'
)


dbconn = DBConnector(conn)

test = dbconn.general_query("select distinct test_id, builder from `testlab-db`.`schedule` where barcode = '1003-BQV000000000000217-1'")
test2 = dbconn.general_query("select distinct test_id, builder from `testlab-db`.`schedule` where barcode = '1003-BQV000000000000247-1'")
test3 = dbconn.fetch_testid('1003-BQV000000000000247-1')

dbconn.close_connection()


