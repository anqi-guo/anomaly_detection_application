SELECT
   污染物ID,
   污染物名称
FROM
   (SELECT YZXH 污染物ID, YZMC 污染物名称, ROW_NUMBER() OVER (PARTITION BY YZXH ORDER BY YZXH) R FROM T_ZXJC_WRY_JCDGLYZ)
WHERE
   R = 1