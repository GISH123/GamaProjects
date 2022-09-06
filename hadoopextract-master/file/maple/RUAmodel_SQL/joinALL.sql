SELECT CHA.characterid AS characterid ,
    CHA.accountID AS accountID,
    CHA.charactername  AS c_name ,
    CHA.b_job AS b_job,
    ACC.count AS acc_num ,
    datediff( '[:Date-N+1]', AGE.registerdate)  AS age ,
    CHA.b_level AS level_f ,

    CHA.exp_f AS exp_f,
    CHA.s_pop AS pop_f ,
    COALESCE (MONEY.money_i,CHA.s_money) AS money_i,
    COALESCE (MONEY.money_f,CHA.s_money) AS money_f,
    COALESCE (MONEY.money_avg, CHA.s_money) AS money_avg,
    COALESCE (MONEY.money_sd,0) AS money_sd,
    CHA.s_charismaexp AS s_charismaexp ,
    CHA.s_insightexp AS s_insightexp ,
    CHA.s_willexp AS s_willexp ,
    CHA.s_craftexp AS s_craftexp ,
    CHA.s_senseexp AS s_senseexp ,
    CHA.s_charmexp AS s_charmexp ,
    STAT.mhp AS m_mhp ,
    STAT.str AS m_str,
    STAT.int AS m_int ,
    STAT.luk AS m_luk ,
    STAT.statdamagemin AS m_statdamagemin ,
    STAT.statdamagemax AS m_statdamagemax,
    STAT.bossdamager AS m_bossdamager,
    STAT.ignoredef AS m_ignoredef,

    COALESCE (INFO.exp_increase,0) AS exp_gain,
    COALESCE (INFO.album_num,0) AS c_albumSize,
    COALESCE (INFO.friend_num,0) AS c_friendnum,
    COALESCE (INFO.meso_buy,0) AS buy_volumn,
    COALESCE (INFO.meso_sell,0) AS sell_volumn,

    COALESCE (G_COM.commitment_diff,0) AS g_commit_gain,
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
    COALESCE (ITEM.i_spl,0) AS i_spl,
    COALESCE (ITEM.i_pzl,0) AS i_pzl,
    COALESCE (ITEM.i_oth,0) AS i_inv,
    COALESCE (ITEM.c_eqp,0) AS ic_eqp,
    COALESCE (ITEM.c_inv,0) AS ic_inv,
    COALESCE (GASH.buy_gash,0) AS buy_gash,
    COALESCE (GASH.gift_gash,0) AS gift_gash,
    W_DAY.online_days AS onlinedays,
    W_DAY.gw_count AS gw_count,
    W_DAY.character_time_ratio AS time_ratio,
    W_DAY.L_SUN AS L_SUN,
    W_DAY.L_MON AS L_MON,
    W_DAY.L_TUE AS L_TUE,
    W_DAY.L_WED AS L_WED,
    W_DAY.L_THU AS L_THU,
    W_DAY.L_FRI AS L_FRI,
    W_DAY.L_SAT AS L_SAT,
    COALESCE(ITEM_TRADE.sell_cnt,0) AS sell_item,
    COALESCE(ITEM_TRADE.buy_cnt,0) AS buy_item,
    --COALESCE(ITEM_TRADE.pass_on,0) AS pass_on_item,
    COALESCE(auc_eqp_buy,0) AS auc_eqp_buy,
    COALESCE(auc_eqp_sell,0) AS auc_eqp_sell,
    COALESCE(auc_fam_buy,0) AS auc_fam_buy,
    COALESCE(auc_fam_sell,0) AS auc_fam_sell,
    COALESCE(auc_oth_buy,0) - COALESCE(auc_fam_buy,0) AS auc_oth_buy,
    COALESCE(auc_oth_sell,0) - COALESCE(auc_fam_sell,0)AS auc_oth_sell,
    COALESCE(auc_cash_buy,0) AS auc_cash_buy,
    COALESCE(auc_cash_sell,0) AS auc_cash_sell

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
ON  W_DAY.world = '[:gw]'
    AND CHA.characterid = W_DAY.characterid

INNER JOIN
	maple_all.gamedb_characterstatinfo STAT
ON
    STAT.world = '[:gw]'
	AND STAT.dt = '[:DateN+1]'
    AND CHA.characterid = STAT.characterid


LEFT JOIN
	gtwpd.maple_middle_monthly_character_info AS INFO
ON
    INFO.world = '[:gw]'
	AND INFO.dt = '[:Date0]'
    AND CHA.characterid = INFO.characterid

LEFT JOIN
	gtwpd.maple_middle_monthly_auction_info AS AUC
ON
    AUC.world = '[:gw]'
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
        AND  BB.world = '[:gw]'
        AND AA.accountid = BB.accountid
)ACC
ON
	ACC.world = '[:gw]'
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
        AND BB.world = '[:gw]'
        AND AA.characterid = BB.characterid
)ITEM
ON
    ITEM.world = '[:gw]'
	AND CHA.characterID = ITEM.characterID


LEFT JOIN
(
	SELECT
		characterid,
		MIN(grade) AS grade,
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
ON  G_COM.world = '[:gw]'
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
ON QUEST.world = '[:gw]'
    AND CHA.characterID = QUEST.characterID

INNER JOIN
(
    SELECT
        sn ,
        gameworldid,
        characterid,
        charactername,
        accountid,
        registerdate

    FROM
        maple_all.globalaccount_character
    WHERE 1=1
        AND dt = '[:DateN+1]'
        AND gameworldid = [:gwInt]
)AGE
ON  CHA.characterID = AGE.characterid

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
    MONEY.world = '[:gw]'
    AND CHA.characterID = MONEY.characterID

LEFT JOIN
(
	SELECT
		buycharactername,
		SUM(buy_price) AS buy_gash,
		SUM(gift_price) AS gift_gash,
		world
	FROM
		gtwpd.maple_middle_cash_income
	WHERE
		dt BETWEEN '[:Date0]' AND '[:DateN]'
		AND world = '[:gw]'
	GROUP BY
		world,
		buycharactername

)GASH
ON GASH.world = '[:gw]'
   AND CHA.characterName = GASH.buycharactername
LEFT JOIN
(
    SELECT
        characterid,
        SUM(sell_cnt) AS sell_cnt,
        SUM(buy_cnt) AS buy_cnt,
        SUM(pass_on) AS pass_on,
        world
    FROM
		gtwpd.maple_middle_item_trade
	WHERE
		dt BETWEEN '[:Date0]' AND '[:DateN]'
		AND world = '[:gw]'
	GROUP BY
		world,
		characterid

)ITEM_TRADE
ON ITEM_TRADE.world = '[:gw]'
AND CHA.characterid = ITEM_TRADE.characterid;
