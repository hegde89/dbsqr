-- Single line comment
/* Single line comment */
/* 
	Multi Line comment:
	mysql> source file_name
	mysql> \. file_name
*/

-- dataSource: number of triples
select triple_ds, count(*) as freq 
into outfile 'd:/dbsqr/testData/ds_triple'
fields terminated by ': '
from triple_table 
group by triple_ds 
order by freq desc;

select triple_ds, count(*) as freq 
into outfile 'd:/dbsqr/testData/ds_objectProperty_freq'
fields terminated by ': '
from triple_table 
where triple_property_type = 2 						/* object property */
group by triple_ds 
order by freq desc;

select triple_ds, count(*) as freq 
into outfile 'd:/dbsqr/testData/ds_dataProperty_freq'
fields terminated by ': '
from triple_table 
where triple_property_type = 1						/* data property */
group by triple_ds 
order by freq desc;


-- entity	concept	ds	 
select entity_id, entity_uri, schema_uri, ds_name  
into outfile 'd:/dbsqr/testData/entity_concept_ds'
fields terminated by '\t'
from entity_table, schema_table, ds_table 
where entity_concept_id = schema_id
and schema_type = 5 								/* concept */		
and entity_ds_id = ds_id
order by entity_id;


-- objectProperty	frequency	ds 	 
select triple_property, count(*) as freq, triple_ds
into outfile 'd:/dbsqr/testData/objectProperty_freq_ds'
fields terminated by '\t'
from triple_table
where triple_property_type = 2						/* object property */
group by triple_property, triple_ds
order by freq desc;


-- dataProperty	frequency	ds
select triple_property, count(*) as freq, triple_ds
into outfile 'd:/dbsqr/testData/dataProperty_freq_ds'
fields terminated by '\t'
from triple_table
where triple_property_type = 1						/* data property */				
group by triple_property, triple_ds
order by freq desc;


-- entity	objectPropertyFreq		concept	ds

