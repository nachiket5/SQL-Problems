
WITH CTE1 AS (
  SELECT 
      USERID
      ,EVENT_TYPE
      ,EVENT_TIME
      ,LAG(EVENT_TIME, 1 , EVENT_TIME) OVER (PARTITION BY USERID ORDER BY EVENT_TIME)  AS PREV_EVENT
      ,TIMESTAMPDIFF(MINUTE, LAG(EVENT_TIME, 1 , EVENT_TIME) OVER 
      (PARTITION BY USERID ORDER BY EVENT_TIME) ,EVENT_TIME) AS DIFFERNCE
      ,ROW_NUMBER() OVER (PARTITION BY USERID ORDER BY EVENT_TIME) AS RNK
  FROM EVENTS
  )
,CTE2 AS (
  SELECT 
      USERID
      ,EVENT_TIME
      ,PREV_EVENT
      ,DIFFERNCE
      ,RNK
      ,CASE WHEN DIFFERNCE <= 30 THEN 0 ELSE 1 END AS FLAGS
      ,SUM(CASE WHEN DIFFERNCE <= 30 THEN 0 ELSE 1 END) OVER (PARTITION BY USERID ORDER BY RNK ) AS SM 
  FROM CTE1 

)

SELECT USERID
      ,SM + 1
      ,MIN(EVENT_TIME)
      ,MAX(EVENT_TIME)
      ,COUNT(*)
      ,TIMESTAMPDIFF(MINUTE,MIN(EVENT_TIME), MAX(EVENT_TIME) ) AS DIIF
FROM CTE2
GROUP BY USERID,SM
