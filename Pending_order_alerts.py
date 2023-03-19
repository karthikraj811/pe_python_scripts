import sheetioRio as rio
import pandas as pd
import datetime as dt

query = """
               select *,convert_timezone('UTC','UTC-5:30',GETDATE()) as current_time,datediff(minutes,order_date,convert_timezone('UTC','UTC-5:30',GETDATE())) as minutes_diff from (select dl.dist_id as distributor_id
 ,dl."name" as distributor_name
 ,convert_timezone('UTC','UTC-5:30',o.order_date) as order_date
 ,oi.fk_id_order as order_id
 ,r.id as customer_code
,r."name" customer_name
,count(*) as total_items
,sum(oi.ordered_quantity) as total_qty
,sum(oi.total) as order_value

FROM rio_oms_omstransactional.order_item oi 
JOIN rio_oms_omstransactional."order" o ON o.id  = oi.fk_id_order AND o.is_active  = 1 AND o.is_deleted  = 0
JOIN rio_oms_omstransactional.order_group og ON og.id  = o.fk_id_order_group 
JOIN rio_oms_omstransactional.retailer r ON r.id  = og.fk_id_retailer AND r.is_active = 1
LEFT JOIN rio_oms_omstransactional.address ra ON ra.id = r.fk_id_address
JOIN rio_oms_omstransactional.distributor d ON d.id  = o.fk_id_distributor AND d.is_active  = 1 AND d.is_blocked  = 0 AND d.is_test  = 0 AND d.id  <> 1900
LEFT JOIN rio_oms_omstransactional.address da ON da.id = d.fk_id_address
JOIN rio_analytics.distributor_list dl ON dl.dist_id  = d.id 
JOIN rio_oms_omstransactional.batch b ON b.id = oi.fk_id_batch AND b.is_active  = 1
JOIN rio_oms_omstransactional.product p ON p.id  = b.fk_id_product AND p.is_active  = 1
JOIN rio_oms_omstransactional.raw_product rp ON rp.distributor_item_code  = b.distributor_item_code AND b.fk_id_distributor  = rp.id_distributor  AND rp.is_active  = b.is_active 
left join rio_oms_omstransactional.manufacturer m on p.fk_id_manufacturer =m.id
where --DATE(convert_timezone('UTC','UTC-5:30',o.order_date)) between '2021-04-01' and '2021-04-30'
r.id  NOT IN (33443,9593,275489,128569,3714800) and dl.dist_id in (2970,5609) and o.order_status =1
group by 1,2,3,4,5,6)
where datediff(minutes,order_date,convert_timezone('UTC','UTC-5:30',GETDATE())) > 30 """




rio_read = rio.getSkull_rio_read()


dfel=pd.read_sql(query,rio_read)
print(dfel)
#dflead = pd.read_sql(query1,con)
driver,sheeter = rio.apiconnect()
if not dfel.empty:
    dfel = dfel.astype(str)
    sheet_id = rio.dftoSheetsfast(driver,sheeter,dfel,
                       sp_nam_id='1PemzqJdaPWZTyKLGoMXFdWfazSUJ7ULnmwjOZCqPh8I',
                       sh_name = 'order_data2',resize_cols = False, resize_rows = False)
    print('output written for eligible retailers')
    print(sheet_id)
else:
    print("empty query output for eligible retailer list")


#from vscode