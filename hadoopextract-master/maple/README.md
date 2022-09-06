# 0 各檔案用途:


    P00_MakeLogin.py
        建立IPLog等登入相關資料
    P10_MakePartition.py
        建立原始區Partition
    P20_MakeExtract.py
        建立中繼站資料
    P30_MakeMiddle.py
        建立建模用中繼站資料(未來將被Model Extract取代)
    P50_MakeALLMonthFirst.py
        建立每月一日之備份
    P70_UseModel.py
        建立真實用戶模型

# 1 登入相關的表格(file/maple/ip_SQL/)
	## 1.1 角色相關
	
	gtwpd.maple_iplog	: 登入登出的原始資料，以dt、world建partition
	login_record		: 角色一次登入登出的時間，如果有跨日則加上ADD
	login_duration_new	: 角色在一日內在線時間統計(包含跨日)
	
	## 1.2 帳號相關
	login_account		: 帳號登入遊戲的原始資料，以dt建partition
	login_ipcount		: iplog中，每一個帳號登入的IP的登入次數、最後登出時間、同IP帳號數、以及最常登入的IP排名
	login_istw			: 每個帳號分別是不是台灣帳號

# 2 營收表格相關(file/maple/model_middle_SQL/)
	cash_acc			:每個帳號每天的消費次數、消費額
	cashitem_desc		:每個商品每天的銷售次數、銷售額

# 3 LOG資料的中繼站表格(file/maple/model_middle_SQL/)

	itemmovepath_log         : 當日進行的物品移動資料。
	auctionhistory_log       : 拍賣完整歷史紀錄。
	auction_familiar_log     : 萌寵拍賣紀錄。
	auction_con_log          : 消耗品拍賣紀錄。
	auction_eqp_log          : 裝備類拍賣紀錄。(包含原本OPT和EQP)
	auction_etc_log          : 其他類拍賣紀錄。
	auction_ins_log          : 裝備類拍賣紀錄。
	auction_cashitem_pet_log : 現金物品-寵物類拍賣紀錄。
	auction_cashitem_eqp_log : 現金物品-現金類拍賣紀錄。
	
# 4 建模中繼站資料(file/maple/model_middle_SQL/)
	cashitem_eqp_log	: 當日有上線的角色現金物品的清單。
	item_log			: 當日有上線的角色物品的清單。
	character_createtime: 當日有上線的角色。
    friend_num			: 當日有上線的角色，好友數。
    album_num    	    : 當日有上線的角色，相簿數。
    mesotrade_buy    	: 當日該角色楓幣購買量。
    mesotrade_sell		: 當日該角色楓幣販賣量。
    guild            	: 當日該角色的公會貢獻量。
    item_used        	: 當日有上線的角色，各狀況道具件數。
    quest_num        	: 當日該角色完成的任務數。
    exp_increase     	: 當日經驗值有增減的角色，經驗值變化量。
    money_increase   	: 當日楓幣有增減的角色，楓幣變化量。
    acc_num          	: 當日有上線的帳號，角色數。
    cash_income      	: 當日商城行為統計表。
    item_trade       	: 當日物品交易行為統計表。
    auction_eqp      	: 當日拍賣物品裝備類的角色，買賣之數量及次數。(15)
    auction_familiar 	: 當日拍賣物品萌寵類的角色，買賣之數量及次數。(13)
    auction_oth      	: 當日拍賣物品萌寵類的角色，買賣之數量及次數。(14(不含13)、16、17)
    auction_cash     	: 當日拍賣物品萌寵類的角色，買賣之數量及次數。(18、19)
