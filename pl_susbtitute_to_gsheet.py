import sheetioRio as rio
import pandas as pd
import datetime as dt

query = """
(
 SELECT sub_id.name "product name",
sub_id.id_substitute,
sub_id.molecular_content,
sub_id.id_mdm as "product_id_mdm",
sub_id.uid_mdm as "Product_uid_mdm",
p.uid_mdm as "Substitute_uid_mdm",
p.name "substitute_product",
COALESCE (m.name,p.manufacturer_name) Manufacturer,
m.id as Manufacturer_id,
p.id_mdm "substitute_id_mdm"
 FROM 
 rio_oms_omstransactional.product p 	
JOIN rio_oms_omstransactional.manufacturer m 
	ON m.id = p.fk_id_manufacturer 
  
  JOIN (
 select distinct p.id_substitute,p.molecular_content,p.id_mdm,p.uid_mdm,p.name from rio_oms_omstransactional.product p 	
JOIN rio_oms_omstransactional.manufacturer m 
	ON m.id = p.fk_id_manufacturer
	where (p."name" ILIKE '%true%cure%' OR m."name" ILIKE '%true%cure%' or p.name ILIKE '%ever%herb%' or m.name ilike '%ever%herb%' or p.name ilike '%liveasy%' or m.name ilike '%liveasy%')
	and p.id_substitute is not null
	
	) sub_id on sub_id.id_substitute = p.id_substitute
)
UNION 
(
SELECT molecule.name "product name",
molecule.id_substitute,
molecule.molecular_content,
molecule.id_mdm as "product_id_mdm",
molecule.uid_mdm as "Product_uid_mdm",
p.uid_mdm "Substitute_uid_mdm",
p.name "substitute_product",
COALESCE (m.name,p.manufacturer_name) Manufacturer,
m.id as Manufacturer_id,
p.id_mdm "substitute_id_mdm"

 FROM 
 rio_oms_omstransactional.product p 	
JOIN rio_oms_omstransactional.manufacturer m 
	ON m.id = p.fk_id_manufacturer 
  
  JOIN (
 select distinct p.id_substitute,p.molecular_content,p.name,p.id_mdm,p.uid_mdm from rio_oms_omstransactional.product p 	
JOIN rio_oms_omstransactional.manufacturer m 
	ON m.id = p.fk_id_manufacturer and  p.molecular_content  not ilike  '%NOT APPLICABLE%' and p.molecular_content  not ilike '%AYURVEDIC%'
	where (p."name" ILIKE '%true%cure%' OR m."name" ILIKE '%true%cure%' or p.name ILIKE '%ever%herb%' or m.name ilike '%ever%herb%' or p.name ilike '%liveasy%' or m.name ilike '%liveasy%')

	
	) molecule on molecule.molecular_content = p.molecular_content	
	
	) """




rio_read = rio.getSkull_rio_read()


dfel=pd.read_sql(query,rio_read)
print(dfel)
#dflead = pd.read_sql(query1,con)
driver,sheeter = rio.apiconnect()
if not dfel.empty:
    dfel = dfel.astype(str)
    sheet_id = rio.dftoSheetsfast(driver,sheeter,dfel,
                       sp_nam_id='1rgpQrQZxvhc5-g_USQcyFXoSqiOK2oK-U7aqE3QVrJw',
                       sh_name = 'substitute_products',resize_cols = False, resize_rows = False)
    print('PL_substitute sheet updated')
    print(sheet_id)
else:
    print("empty dataframe")