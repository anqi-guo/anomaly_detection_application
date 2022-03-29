SELECT
   DISTINCT 排口ID, 监测时间, 流量, 是否生产
FROM
   t_zxyc_{}_{}_processed
WHERE
   监测时间 BETWEEN '{}' AND '{}'