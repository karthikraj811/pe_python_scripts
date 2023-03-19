


(SELECT  order_id,name,Ucode,batch_id, dist_name,"Order Placement Time", DATE(convert_timezone('UTC','UTC-5:30',"Order Placement Time")),ordered_quantity,order_quantity,free_quantity,date_rnk,qty_rank 
FROM
(
select 
o.id as order_id,
oi.ordered_quantity, 
rp.distributor_item_code as "Ucode",
d.name dist_name,
p.name,
o.order_date as "Order Placement Time" ,
b.id as batch_id,
oi.fk_id_scheme as order_scheme_id,
sj.id as scheme_id,
rp.scheme,
sj.order_quantity::text,
sj.free_quantity::text,
concat(sj.discount_percentage::text,'%') as discount_percentage,
ROW_NUMBER()over(partition by o.id,b.id,sj.order_quantity order by sj.last_modified_date desc) as date_rnk,
ROW_NUMBER()over(partition by o.id,rp.distributor_item_code order by sj.order_quantity desc) as qty_rank
from rio_oms_omstransactional.order_item oi 
join rio_oms_omstransactional."order" o  on oi.fk_id_order = o.id and o.is_active =1 and o.is_deleted =0
JOIN rio_oms_omstransactional.order_group og ON og.id  = o.fk_id_order_group 
left join rio_oms_omstransactional.distributor d on d.id = o.fk_id_distributor and d.is_active =1 and d.is_blocked =0 and d.is_test =0 and d.id <> 1900
left join rio_oms_omstransactional.batch b on b.id = oi.fk_id_batch and b.is_active =1
left join rio_oms_omstransactional.scheme_journal sj on sj.fk_id_batch =b.id and sj.last_modified_date <= o.order_date and sj.order_quantity <= oi.ordered_quantity and sj.is_active =1
left join rio_oms_omstransactional.product p on p.id = b.fk_id_product and p.is_active =1
left JOIN rio_oms_omstransactional.raw_product rp ON rp.distributor_item_code  = b.distributor_item_code 
AND b.fk_id_distributor  = rp.id_distributor  AND rp.is_active  = b.is_active 
left join rio_oms_omstransactional.manufacturer m2 
on p.fk_id_manufacturer =m2.id
where  
o.id = 30438776 
)aa
where 
date_rnk =1 and qty_rank =1
order by 1
)