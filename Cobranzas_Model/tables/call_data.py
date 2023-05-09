
import pandas as pd
import pymysql

conn=pymysql.connect(host='54.175.78.29',port=int(3306),user='fivreaduser',passwd='0Q4W3@pE^pb5Nu',db='dbFactorClickProd')


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
#dato del par: el documento que pagó un documento
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
d.last_management , d.last_management_date , d.normalization_executive_name, d.normalization_executive_id, d.last_payment_date , d.custom_expiration_utc 
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
d.last_management , d.last_management_date , d.normalization_executive_name, d.normalization_executive_id, d.last_payment_date , d.custom_expiration_utc 
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
	last_management , last_management_date , normalization_executive_name, normalization_executive_id, last_payment_date , custom_expiration_utc
	from df
	group by document_id 
	
"""

data = pd.read_sql(data_query, conn)
