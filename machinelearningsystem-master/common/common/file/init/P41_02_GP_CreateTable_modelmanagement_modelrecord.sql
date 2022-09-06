DROP TABLE modelmanagement.modelrecord  ;

DROP sequence modelmanagement.modelrecord_modelrecordid_seq ;

create sequence modelmanagement.modelrecord_modelrecordid_seq ;

CREATE TABLE modelmanagement.modelrecord (
	createtime timestamp NULL,
	modifytime timestamp NULL,
	deletetime timestamp NULL,
	modelrecordid INTEGER NOT NULL PRIMARY KEY default nextval('modelmanagement.modelrecord_modelrecordid_seq'),
	modelversion text NULL,
	runstep text NULL,
	parameterjson text NULL,
	resultjson text null ,
	state text null
);

