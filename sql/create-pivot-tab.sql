select load_extension('/Users/pete/GitHub/particle_flux_database/pivotvtab.dylib');

drop table ct;

--select DISTINCT var_id, concat(var_id, ' ', variables.name, ' (', units, ')') from variables join file_variables USING (var_id) order by 1;

create virtual table ct using pivot_vtab (
	-- rows
	(select distinct (file_id * 1000000) + sample_id AS file_sample_id from data),
	--columns
	(select var_id, concat(var_id, ' ', variables.name) from variables) ,
	-- data
	(select value from data where (file_id * 1000000) + sample_id  = ?1 and var_id = ?2)
);

select * from ct;
