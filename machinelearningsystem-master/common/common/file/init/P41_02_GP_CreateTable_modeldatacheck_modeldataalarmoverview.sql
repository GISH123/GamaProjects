DROP TABLE modeldatacheck.modeldataalarmoverview  ;

DROP sequence modeldatacheck.modeldataalarmoverview_modeldataalarmoverviewid_seq ;

create sequence modeldatacheck.modeldataalarmoverview_modeldataalarmoverviewid_seq ;

CREATE TABLE modeldatacheck.modeldataalarmoverview (
	createtime timestamp NULL,
	modifytime timestamp NULL,
	deletetime timestamp NULL,
	modeldataalarmoverviewid INTEGER NOT NULL PRIMARY KEY default nextval('modeldatacheck.modeldataalarmoverview_modeldataalarmoverviewid_seq'),
	productname text NULL,
	project text NULL,
	datadate date NULL,
	errorcount INTEGER NULL,
	lv_a_errorcount INTEGER NULL,
	lv_b_errorcount INTEGER NULL,
	lv_c_errorcount INTEGER NULL,
	lv_d_errorcount INTEGER NULL,
	lv_e_errorcount INTEGER NULL,
	lv_f_errorcount INTEGER NULL,
	lv_g_errorcount INTEGER NULL,
	lv_h_errorcount INTEGER NULL
) ;




