# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 12:48:06 2024

@author: EugenioCalandrini
"""

import pandas as pd
import random
import mysql.connector
import base64
import numpy as np
import matplotlib.pyplot as plt
from DBconnector import DBConnector
from Cell import Cell, Cells

def cell_list(ids):
    """This function generates a list with BQV cell ids (without the prefix and suffix)

    Args:
        ids (list): a list with the number of the cell (without all the beginning zeros)

    Returns:
        list: all the ids with the the format BQV000000000000001
    """
    return ["BQV{:015d}".format(id) for id in ids]


# coincells = Cells(["RDCCDAZ1LIM50M1503001"])
# coincells.plot_cycling()


# puchcells = Cells(cell_list([38,39]))
# puchcells.plot_cycling()
# test3.plot_formation()

# puchcells = Cells(['1003-BQV000000000000067-1', '1003-BQV000000000000068-1',
#        '1003-BQV000000000000069-1', '1003-BQV000000000000147-1',
#        '1003-BQV000000000000156-1', '1003-BQV000000000000155-1',
#        '1003-BQV000000000000148-1', '1003-BQV000000000000153-1',
#        '1003-BQV000000000000149-1', '1003-BQV000000000000152-1',
#        '1003-BQV000000000000150-1', '1003-BQV000000000000151-1',
#        '1003-BQV000000000000154-1', '1003-BQV000000000000244-1',
#        '1003-BQV000000000000245-1', '1003-BQV000000000000246-1',
#        '1003-BQV000000000000251-1', '1003-BQV000000000000250-1',
#        '1003-BQV000000000000252-1'])
# puchcells.plot_cycling()


<<<<<<< HEAD
ttt = Cells(cell_list([38]))
ttt.plot_formation()
=======
ttt = Cells(cell_list([68]))
# ttt.plot_formation()
ttt.cells[0].cycle.head()
>>>>>>> 225317829cfa3b870c1bea3f04386eae3e851d77
