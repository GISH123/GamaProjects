SELECT
    CHA.characterid AS characterid ,
    CHA.accountID AS accountID,
    W_DAY.online_days AS onlinedays,
    CHA.charactername  AS c_name ,
    ACC.count AS acc_num ,
    AGE.registerdate  AS c_age ,
    COALESCE (EXP.level_i,CHA.b_level) AS level_i ,
    CHA.b_level AS level_f ,
    COALESCE (EXP.exp_i, CHA.exp_f) AS exp_i ,
    CHA.exp_f AS exp_f,
    CHA.s_pop AS pop_f ,
    COALESCE (MONEY.money_i,CHA.s_money) AS money_i,
    COALESCE (MONEY.money_f,CHA.s_money) AS money_f,
    COALESCE (MONEY.money_avg, CHA.s_money) AS money_avg,
    COALESCE (MONEY.money_sd,0) AS money_sd,

    CHA.s_charismaexp AS charismaexp ,
    CHA.s_insightexp AS insightexp ,
    CHA.s_willexp AS willexp ,
    CHA.s_craftexp AS craftexp ,
    CHA.s_senseexp AS senseexp ,
    CHA.s_charmexp AS charmexp ,
    STAT.mhp AS m_mhp ,
    STAT.str AS m_str,
    STAT.int AS m_int ,
    STAT.luk AS m_luk ,
    STAT.statdamagemin AS m_statdamagemin ,
    STAT.statdamagemax AS m_statdamagemax,
    STAT.bossdamager AS m_bossdamager,
    STAT.ignoredef AS m_ignoredef,

    COALESCE (INFO.album_num,0) AS albumSize,
    COALESCE (INFO.friend_num,0) AS friendnum,
    COALESCE (G_COM.commitment_i,0) AS g_commit_i,
    COALESCE (G_COM.commitment_f,0) AS g_commit_f,
    COALESCE (G_COM.grade,6) AS g_grade,

    COALESCE (QUEST.q_daily,0) AS q_daily,
    COALESCE (QUEST.q_milestone,0) AS q_milestone,
    COALESCE (QUEST.q_mday ,0) AS q_mday,
    COALESCE (QUEST.q_happyday,0) AS q_happyday,
    COALESCE (QUEST.q_battlefield,0) AS q_battlefield,
    COALESCE (QUEST.q_wulin,0) AS q_wulin,
    COALESCE (QUEST.q_other,0) AS q_other,

    COALESCE (ITEM.i_atk,0) AS i_atk,
    COALESCE (ITEM.i_def,0) AS i_def,
    COALESCE (ITEM.i_acc,0) AS i_acc,
    0 AS i_spl,
    COALESCE (ITEM.i_pzl,0) AS i_pzl,

    COALESCE (INFO.meso_buy,0) AS buy_volumn,
    COALESCE (INFO.meso_sell,0) AS sell_volumn

FROM
    (
    SELECT
        characterid,
        accountid,
        charactername,
        b_job,
        b_level,
        s_money,
        AA.s_exp + CC.allexp AS exp_f,
        s_pop,
        s_charismaexp,
        s_insightexp,
        s_willexp,
        s_craftexp,
        s_senseexp,
        s_charmexp,
        world,
        dt
    FROM
	    maple_all.gamedb_character AA
	INNER JOIN gtwpd.maple_othertable_levelexp CC
	    ON AA.b_level = CC.level
	WHERE world = '[:gw]'
        AND dt = '[:DateN+1]'

    )CHA

INNER JOIN
(
    SELECT
        characterid,
        online_days,
        gw_count,
        character_time_ratio,
        L_SUN,
        L_MON,
        L_TUE,
        L_WED,
        L_THU,
        L_FRI,
        L_SAT,
        world
    FROM
        gtwpd.maple_middle_monthly_login_info
    WHERE
        world = '[:gw]'
        AND dt = '[:Date0]'

)W_DAY
ON CHA.world = W_DAY.world
    AND CHA.characterid = W_DAY.characterid

INNER JOIN
	maple_all.gamedb_characterstatinfo AS STAT
ON
    CHA.world = STAT.world
	AND CHA.dt = STAT.dt
    AND CHA.characterid = STAT.characterid


LEFT JOIN
	gtwpd.maple_middle_monthly_character_info AS INFO
ON
    CHA.world = INFO.world
	AND INFO.dt = '[:Date0]'
    AND CHA.characterid = INFO.characterid

LEFT JOIN
(
    SELECT
        min(exp_i) AS exp_i,
        max(exp_f) AS exp_f,
        min(level_i) AS level_i,
    	max(level_f) AS level_f,
    	characterid,
    	world
    FROM
	    gtwpd.maple_middle_exp_increase
	WHERE
        dt BETWEEN  '[:Date0]' AND '[:DateN]'
        AND world = '[:gw]'
    group by characterid , world
)EXP
ON
    CHA.characterid = EXP.characterid

LEFT JOIN
	gtwpd.maple_middle_monthly_auction_info AS AUC
ON
    CHA.world = AUC.world
	AND AUC.dt =  '[:Date0]'
	AND CHA.accountid = AUC.accountid
    AND CHA.characterid = AUC.characterid


LEFT JOIN
(
    SELECT
        BB.accountid AS accountid,
        BB.count AS count,
        BB.dt AS dt,
        BB.world AS world
    FROM
    (
        SELECT
            MAX(dt) AS dt,
            accountid,
            world
        FROM
	        gtwpd.maple_middle_acc_num
	    WHERE world = '[:gw]'
	        AND dt BETWEEN '[:Date0]' AND '[:DateN]'
        GROUP BY world , accountid
    )AA
    INNER JOIN
        gtwpd.maple_middle_acc_num BB
    ON
        AA.dt = BB.dt
        AND AA.world = BB.world
        AND AA.accountid = BB.accountid
)ACC
ON
	CHA.world = ACC.world
    AND CHA.accountID = ACC.accountID

LEFT JOIN
(
    SELECT
        BB.characterid AS characterid,
        BB.i_atk AS i_atk,
        BB.i_def AS i_def,
        BB.i_acc AS i_acc,
        BB.i_spl AS i_spl,
        BB.i_pzl AS i_pzl,
        BB.i_oth AS i_oth,
        BB.c_eqp AS c_eqp,
        BB.c_inv AS c_inv,
        BB.dt AS dt,
        BB.world AS world
    FROM
    (
        SELECT
            MAX(dt) AS dt,
            characterid,
            world
        FROM
	        gtwpd.maple_middle_item_used
	    WHERE world = '[:gw]'
	        AND dt BETWEEN '[:Date0]' AND '[:DateN]'
        GROUP BY world, characterid
    )AA
    INNER JOIN
        gtwpd.maple_middle_item_used AS BB
    ON
        AA.dt = BB.dt
        AND AA.world = BB.world
        AND AA.characterid = BB.characterid
)ITEM
ON
    CHA.world = ITEM.world
	AND CHA.characterID = ITEM.characterID


LEFT JOIN
(
	SELECT
		characterid,
		MIN(grade) AS grade,
		MIN(commitment_i) AS commitment_i,
		SUM(commitment_diff) AS commitment_diff,
		MAX(commitment_f) AS commitment_f,
		world
	FROM
		gtwpd.maple_middle_guild
	WHERE
		dt BETWEEN '[:Date0]' AND '[:DateN]'
		AND world = '[:gw]'
	GROUP BY
		world, characterid

)G_COM
ON  CHA.world = G_COM.world
    AND CHA.characterID = G_COM.characterID

LEFT JOIN
(
	SELECT
		characterid,
		SUM(q_daily) AS q_daily,
		SUM(q_milestone) AS q_milestone,
		SUM(q_mday) AS q_mday,
		SUM(q_happyday) AS q_happyday,
		SUM(q_wulin) AS q_wulin,
		SUM(q_battlefield) AS q_battlefield,
		SUM(q_other) AS q_other,
		world
	FROM
		 gtwpd.maple_middle_quest_num
	WHERE
		dt BETWEEN '[:Date0]' AND '[:DateN]'
		AND world = '[:gw]'
	GROUP BY
		world, characterid

)QUEST

ON CHA.world = QUEST.world
    AND CHA.characterID = QUEST.characterID


INNER JOIN
    gtwpd.maple_middle_character_age AS AGE
ON  AGE.world = '00'
	AND AGE.cid_key = CHA.characterID %10
	AND CHA.characterID = AGE.characterid

LEFT JOIN
(
	SELECT
		characterid,
		stddev_samp(money_gain) AS money_sd,
		AVG(money_f) AS money_avg,
		MIN(money_f) AS money_i,
		MAX(money_f) AS money_f,
		world
	FROM
		gtwpd.maple_middle_money_increase
	WHERE
		dt BETWEEN '[:Date0]' AND '[:DateN]'
		AND world = '[:gw]'
	GROUP BY
		world, characterid

)MONEY
ON
    CHA.world = MONEY.world
    AND CHA.characterID = MONEY.characterID

;