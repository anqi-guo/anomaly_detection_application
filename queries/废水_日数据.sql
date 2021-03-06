SELECT
	JCZ.JCDXH 排口ID,
	JCZ.WRWBH 污染物ID,
	JCZ.JCSJ 监测时间,
	JCZ.JCZ 污染物浓度值,
	JCZ.WRWPFL 污染物排放量,
	JCJ.LLORPFLXYZ 排放量,
	JCJ.PJLLORPFLXYZ 流量
FROM
	( SELECT JCDXH, WRWBH, XYZ AS JCZ, JCSJ, WRWPFL FROM T_ZXJC_JCSJ_FS_RSJ_JCZ WHERE JCZ IS NOT NULL ) JCZ
	LEFT JOIN ( SELECT XTXH, JCSJ, LLORPFLXYZ, PJLLORPFLXYZ FROM T_ZXJC_JCSJ_FS_RSJ_JCJ ) JCJ ON JCZ.JCDXH = JCJ.XTXH AND JCZ.JCSJ = JCJ.JCSJ
	INNER JOIN ( SELECT DISTINCT XH, SHSJ FROM T_ZXJC_WRY_JCD WHERE JCDMC NOT LIKE '%进水%' AND JCDMC NOT LIKE '%雨水%' AND SHZT = 1 ) PKMC ON JCZ.JCDXH = PKMC.XH
	INNER JOIN ( SELECT DISTINCT JCDXH FROM T_ZXJC_WRY_JCDGLYZ ) VALIDPK ON JCZ.JCDXH = VALIDPK.JCDXH
WHERE
	JCZ.JCSJ BETWEEN to_date( SUBSTR( '{}', 1, 19 ), 'yyyy-MM-dd HH24:mi:ss' )
	AND to_date( SUBSTR( '{}', 1, 19 ), 'yyyy-MM-dd HH24:mi:ss' )
    AND JCZ.JCSJ > PKMC.SHSJ + NUMTODSINTERVAL(7, 'DAY')