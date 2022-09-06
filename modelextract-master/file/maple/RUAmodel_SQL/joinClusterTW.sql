SELECT
    AA.*,
	cluster,
	"F1",
	"NF1",
	item1,
	wd1,
	auc_buy1,
	auc_sell1,
	"nameEntropy" , 
	serviceaccountid, 
	twhk
FROM
    model_rua.[:DataName]_[:GW]_[:Date0] AA
INNER JOIN
    result_rua.[:ModelName]_group_[:Date0] BB
ON AA.characterid = BB.characterid
AND BB.gw = '[:GW]'
INNER JOIN
(
    SELECT
        serviceaccountid,
        accountid,
        CASE WHEN mainaccount IS NULL THEN 'HK' ELSE 'TW' END AS twhk
    FROM
        otherdata.beanfun_account_[:Date0]
)CC
ON AA.accountid = CC.accountid
;