DROP TABLE modeldatacheck.modeldatacheckinfo  ;

DROP sequence modeldatacheck.modeldatacheckinfo_modeldatacheckinfoid_seq ;

create sequence modeldatacheck.modeldatacheckinfo_modeldatacheckinfoid_seq ;

CREATE TABLE modeldatacheck.modeldatacheckinfo (
	createtime timestamp NULL,
	modifytime timestamp NULL,
	deletetime timestamp NULL,
	modeldatacheckinfoid INTEGER NOT NULL PRIMARY KEY default nextval('modeldatacheck.modeldatacheckinfo_modeldatacheckinfoid_seq'),
	productname text NULL,
	project text NULL,
	step text NULL,
	version text NULL,
	datadate date NULL,
	checkcolumn text NULL,
	checkfunc text NULL,
	checknumbervalue double precision NULL,
	checktextvalue text NULL
) ;


