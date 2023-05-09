# -*- coding: utf-8 -*-
"""Modeling Required
"""


import pandas as pd
import numpy as np
import pymysql
from ast import literal_eval




class recursive:
  def __init__(self, car, rec):
    self.car = car
    self.rec = rec
  
  def firstIteration(self):
    """Interescta lo que está en cartera y pagado con documentos, 
    relacionando el documento pasado que fue pagado con el que está en cartera"""
    car = self.car
    rec = self.rec
    start = pd.merge(car, rec, left_on = 'document_id', right_on = 'document_id')
    start.rename(columns = {'document_id':'1_document_id', 'past_document_id': '2_document_id'}, inplace = True)
    start['n_iteration'] = 1
    return start

  def iterations(self):
    """Itera entre el documento pasado y el anterior, 
    hasta llegar al original para cada doc"""
    car = self.car
    rec = self.rec
    n = 2
    start_to_merge = recursive(car, rec).firstIteration()
    second = recursive(car, rec).firstIteration()
    while len(second['1_document_id'].unique())>0:

          second['n_iteration'] = n 
          second = pd.merge(second, rec[['document_id', 'past_document_id']], left_on = str(n) + '_document_id', right_on = 'document_id')
          
          second.rename(columns = {'past_document_id': str(n+1) + '_document_id'}, inplace = True)
          second = second.drop_duplicates().drop(columns = ['document_id'])
          start_to_merge = pd.concat([start_to_merge, second], axis = 0, ignore_index=True)
          n = n + 1
    return start_to_merge


def branches(df):
  """Elige solo las ramas completas"""
  #máxima iteración por documento
  df1 = df.groupby(['1_document_id', 'n_iteration']).count().reset_index().groupby('1_document_id')['n_iteration'].max().to_frame().reset_index() 
  #elige las ramas que están con la máxima iteración por doc
  onlys = pd.merge(df, df1, left_on = ['1_document_id', 'n_iteration'], right_on = ['1_document_id', 'n_iteration'], how = 'inner')
  return onlys.filter(regex = 'document|iteration')


def carteraAndOriginal(onlys):
  #first document per row, so the cartera document
  cartera_doc = onlys['1_document_id']
  #last document per row, so the original document
  original_doc = onlys.filter(regex='document').ffill(axis=1).iloc[:, -1] 
  #unimos documento original con el de la cartera para un buen trackeo
  cartera_original = pd.concat([cartera_doc, original_doc], axis = 1).drop_duplicates()
  return cartera_original


#ramas hacia el lado con data
def branchesToSide(complete_branches, data): 
  """Poner las ramas hacia el lado
  permite comprender de mejor manera 
  la historia del cobro"""
  d = {}
  for i in complete_branches.filter(regex='document').columns:
    d[i] = data[data['document_id'].isin(complete_branches[i].tolist())]
    d[i] = d[i].add_prefix(str(i[:2]))
    complete_branches = pd.merge(complete_branches, d[i], left_on = i, right_on = i, how = 'left')
  return complete_branches


#ramas hacia abajo con data 
def branchesDown(complete_branches, data): 
  """Poner las ramas hacia abajo
  permite un mejor filtrado de los documentos"""
  e = {}
  df_concats = pd.DataFrame()
  for i in complete_branches.filter(regex='document').columns:
    e[i] = data[data['document_id'].isin(complete_branches[i].tolist())]
    complete_branches_toconcat_data = pd.merge(complete_branches[[i]], e[i], left_on = i, right_on = i[2:], how = 'left')
    complete_branches_toconcat_data.rename(columns = {i:'document_id'})
    df_concats = pd.concat([complete_branches_toconcat_data, df_concats])
  return df_concats.dropna(subset = ['document_id']).drop_duplicates()


