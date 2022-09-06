DROP TABLE IF EXISTS modelmanagement.modelversion  ;

DROP sequence IF EXISTS modelmanagement.modelversion_modelversionid_seq ;

create sequence modelmanagement.modelversion_modelversionid_seq ;

CREATE TABLE modelmanagement.modelversion (
	createtime timestamp NULL,
	modifytime timestamp NULL,
	deletetime timestamp NULL,
	modelversionid INTEGER NOT NULL PRIMARY KEY default nextval('modelmanagement.modelversion_modelversionid_seq'),
	builduser TEXT NULL ,
	builddate date NULL ,
	productname text NULL,
	project text NULL,
	modelVersion text NULL,
	modelVersionFullCode text NULL,
	rawdataVersion text NULL,
	preprocessVersion text NULL,
	usemodelVersion text NULL,
	runstep text NULL,
	parameterjson text NULL,
	resultjson text NULL
) ;


