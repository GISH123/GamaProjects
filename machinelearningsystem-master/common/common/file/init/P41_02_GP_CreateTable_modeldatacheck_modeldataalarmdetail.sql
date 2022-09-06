DROP TABLE modeldatacheck.modeldataalarmdetail  ;

DROP sequence modeldatacheck.modeldataalarmdetail_modeldataalarmdetailid_seq ;

create sequence modeldatacheck.modeldataalarmdetail_modeldataalarmdetailid_seq ;

CREATE TABLE modeldatacheck.modeldataalarmdetail (
	createtime timestamp NULL ,
	modifytime timestamp NULL ,
	deletetime timestamp NULL ,
	modeldataalarmdetailid INTEGER NOT NULL PRIMARY KEY default nextval('modeldatacheck.modeldataalarmdetail_modeldataalarmdetailid_seq'),
	productname text NULL ,
	project text NULL ,
	step text NULL ,
	version text NULL ,
	datadate date NULL ,
	checkcolumn text NULL ,
	checkfunc text NULL ,
	alarmfunc text NULL ,
	alarmlevel text NULL ,
	currentvalue double precision NULL ,
	alarmmessage text NULL
) ;

