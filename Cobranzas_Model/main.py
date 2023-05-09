# -*- coding: utf-8 -*-
"""Execute models to Obtain DataBases
"""

from tables.call_data import data, car, rec 
from models.scripts import recursive, branches, carteraAndOriginal, branchesToSide, branchesDown
import pandas as pd
import numpy as np


recurs = recursive(car, rec).iterations()

complete_branches = branches(recurs)

cartera_original = carteraAndOriginal(complete_branches)

#ramas hacia el lado con data
complete_branches_with_data = branchesToSide(complete_branches, data)

#ramas hacia abajo con data 
df_concats = branchesDown(complete_branches, data)

#tabla de documentos en cartera relacionados con documento original
c_o1 = pd.merge(cartera_original, data.add_prefix('car_'), left_on = '1_document_id', right_on = 'car_document_id', how = 'left')
c_19 = pd.merge(c_o1, data.add_prefix('ori_'), left_on = '9_document_id', right_on = 'ori_document_id', how = 'left')

