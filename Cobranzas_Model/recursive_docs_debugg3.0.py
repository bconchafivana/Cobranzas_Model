# -*- coding: utf-8 -*-
"""recursive_docs_debugg3.0.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1hrz9pWzeeNC0RRR1Wy8DksEC4EPKmbpg
"""

pip install pymysql

import pandas as pd
import numpy as np
import pymysql
from ast import literal_eval
conn=pymysql.connect(host='54.175.78.29',port=int(3306),user='fivreaduser',passwd='0Q4W3@pE^pb5Nu',db='dbFactorClickProd')
print(conn)

from sqlalchemy import create_engine

"""# Cartera Disponible"""

cartera_query = """
select  d.document_id
 from fc_operations o
 inner join fc_rf_operation_documents od on o.operation_id=od.operation_id
 inner join fc_documents d on d.document_id = od.document_id
 WHERE d.operation IS NOT NULL AND d.status = 2
 AND d.backoffice_status != 'Cas'
 AND (d.deleted is null OR d.deleted = 0)
 AND financed_balance > 1000000"""
 
car = pd.read_sql(cartera_query, conn)

"""# Ancestors

"""

rec_query = """

with df as (
		with ancestor as (
		-- documentos que han sido pagados con operaciones de renegociación
select	pd.document_id , max(pm.reference) as operacion_paga, pd.finance_amount  
from	fc_rf_payment_documents pd
left join fc_rf_payment_means pm on pm.payment_id = pd.payment_id 
where	pd.payment_id in (
	select	distinct payment_id
	from	fc_rf_payment_means mp
	inner join fc_operations op on mp.reference = op.no_operation
	where	mp.document_type = 201 
	 and	op.deleted is null
	 and	op.status = 3
)
 group by pd.document_id 
)
-- documento que pagó y documento que fue pagado
select d.document_id , ancestor.document_id as past_document_id, d.financed_balance , d.finance_amount 
from
fc_operations op 
inner join ancestor on ancestor.operacion_paga = op.no_reception 
	inner join fc_rf_operation_documents ods on ods.operation_id = op.operation_id 
	inner join fc_documents d on d.document_id = ods.document_id 
	)
	select document_id , past_document_id, financed_balance , finance_amount 
	from df"""

rec = pd.read_sql(rec_query, conn)

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

recurs = recursive(car, rec).iterations()

def branches(df):
  """Elige solo las ramas completas"""
  #máxima iteración por documento
  df1 = df.groupby(['1_document_id', 'n_iteration']).count().reset_index().groupby('1_document_id')['n_iteration'].max().to_frame().reset_index() 
  #elige las ramas que están con la máxima iteración por doc
  onlys = pd.merge(df, df1, left_on = ['1_document_id', 'n_iteration'], right_on = ['1_document_id', 'n_iteration'], how = 'inner')
  return onlys.filter(regex = 'document|iteration')

complete_branches = branches(recurs)

def carteraAndOriginal(onlys):
  #first document per row, so the cartera document
  cartera_doc = onlys['1_document_id']
  #last document per row, so the original document
  original_doc = onlys.filter(regex='document').ffill(axis=1).iloc[:, -1] 
  #unimos documento original con el de la cartera para un buen trackeo
  cartera_original = pd.concat([cartera_doc, original_doc], axis = 1).drop_duplicates()
  return cartera_original

cartera_original = carteraAndOriginal(complete_branches)

"""# Agregar datos por documento"""

#extraemos data solo de los documentos que han sido pagados con documentos
data_query = """
 with df as (
		with ancestor as (
select	pd.document_id , max(pm.reference) as operacion_paga, pd.finance_amount  
from	fc_rf_payment_documents pd
left join fc_rf_payment_means pm on pm.payment_id = pd.payment_id 
where	pd.payment_id in (
	select	distinct payment_id
	from	fc_rf_payment_means mp
	inner join fc_operations op on mp.reference = op.no_operation
	where	mp.document_type = 201 
	 and	op.deleted is null
	 and	op.status = 3
)
 group by pd.document_id 
)
select d.document_id , d.client_rut , d.client_name , d.debtor_rut , d.debtor_name ,
	FROM_UNIXTIME(emission) as emission_date,  d.folio , d.backoffice_status , 
CASE WHEN d.sii_executive_merit = 1 THEN 1   
	ELSE 0 END AS merit,
d.financed_balance , d.finance_amount , ancestor.finance_amount as original_finance_amount,
DATEDIFF(CURDATE(), d.custom_expiration_utc) as mora_days , d.debtor_category , d.judicial_cause_id , d.document_type , 
d.last_management , d.last_management_date , d.normalization_executive_name, d.normalization_executive_id, d.last_payment_date 
from
fc_operations op 
-- datos del que pagó un doc
inner join ancestor on ancestor.operacion_paga = op.no_reception 
	inner join fc_rf_operation_documents ods on ods.operation_id = op.operation_id 
	inner join fc_documents d on d.document_id = ods.document_id 

	UNION ALL 
	select ancestor.document_id, d.client_rut , d.client_name , d.debtor_rut , d.debtor_name ,
	FROM_UNIXTIME(emission) as emission_date,   d.folio , d.backoffice_status , 
CASE WHEN d.sii_executive_merit = 1 THEN 1   
	ELSE 0 END AS merit,
d.financed_balance , d.finance_amount , ancestor.finance_amount as original_finance_amount, 
DATEDIFF(CURDATE(), d.custom_expiration_utc) as mora_days , d.debtor_category , d.judicial_cause_id , d.document_type ,
d.last_management , d.last_management_date , d.normalization_executive_name, d.normalization_executive_id, d.last_payment_date 
FROM ancestor
-- datos del ancestor
LEFT JOIN fc_documents d ON d.document_id = ancestor.document_id
	)
	select document_id , avg(financed_balance) as financed_balance  , avg(finance_amount) as finance_amount ,
	client_rut as client_rut , client_name as client_name , debtor_rut as debtor_rut , 
	debtor_name as debtor_name , avg(merit) as merit,
	emission_date, folio, backoffice_status, mora_days, 
	CASE WHEN mora_days <= 0 THEN 'Vigente'
	WHEN mora_days < 30 THEN 'Mora 30'
	WHEN mora_days < 60 THEN 'Mora 60'
	WHEN mora_days < 120 THEN 'Mora 120'
	WHEN mora_days <= 365 THEN 'Mora 365'
	ELSE 'Mora > 365' END AS mora_category, debtor_category,
	CASE WHEN debtor_category = 'P' THEN 1
	ELSE 0 END AS gobierno, judicial_cause_id, document_type,
	last_management , last_management_date , normalization_executive_name, normalization_executive_id, last_payment_date 
	from df
	group by document_id 
	
"""
data = pd.read_sql(data_query, conn)

#ramas hacia el lado con data
def branches_to_side(complete_branches): 
  """Poner las ramas hacia el lado
  permite comprender de mejor manera 
  la historia del cobro"""
  d = {}
  for i in complete_branches.filter(regex='document').columns:
    d[i] = data[data['document_id'].isin(complete_branches[i].tolist())]
    d[i] = d[i].add_prefix(str(i[:2]))
    complete_branches = pd.merge(complete_branches, d[i], left_on = i, right_on = i, how = 'left')
  return complete_branches
complete_branches_with_data = branches_to_side(complete_branches)

#ramas hacia abajo con data 
def branches_down(complete_branches, data): 
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
df_concats = branches_down(complete_branches, data)

#tabla de documentos en cartera relacionados con documento original
c_o1 = pd.merge(cartera_original, data.add_prefix('car_'), left_on = '1_document_id', right_on = 'car_document_id', how = 'left')
c_19 = pd.merge(c_o1, data.add_prefix('ori_'), left_on = '9_document_id', right_on = 'ori_document_id', how = 'left')

