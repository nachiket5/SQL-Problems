create table events 
(userid int , 
event_type varchar(20),
event_time datetime);

insert into events VALUES (1, 'click', '2023-09-10 09:00:00');
insert into events VALUES (1, 'click', '2023-09-10 10:00:00');
insert into events VALUES (1, 'scroll', '2023-09-10 10:20:00');
insert into events VALUES (1, 'click', '2023-09-10 10:50:00');
insert into events VALUES (1, 'scroll', '2023-09-10 11:40:00');
insert into events VALUES (1, 'click', '2023-09-10 12:40:00');
insert into events VALUES (1, 'scroll', '2023-09-10 12:50:00');
insert into events VALUES (2, 'click', '2023-09-10 09:00:00');
insert into events VALUES (2, 'scroll', '2023-09-10 09:20:00');
insert into events VALUES (2, 'click', '2023-09-10 10:30:00');


with cte1 as (
  select 
      userid
      ,event_type
      ,event_time
      ,lag(event_time, 1 , event_time) over (partition by userid order by event_time)  as prev_event
      ,TIMESTAMPDIFF(MINUTE, lag(event_time, 1 , event_time) over 
      (partition by userid order by event_time) ,event_time) as differnce
      ,row_number() over (partition by userid order by event_time) as rnk
  from events
  )
,cte2 as (
  select 
      userid
      ,event_time
      ,prev_event
      ,differnce
      ,rnk
      ,case when differnce <= 30 then 0 else 1 end as flags
      ,SUM(case when differnce <= 30 then 0 else 1 end) OVER (partition BY userid ORDER BY RNK ) AS SM 
  from cte1 

)

SELECT userid
      ,SM + 1
      ,MIN(event_time)
      ,MAX(event_time)
      ,COUNT(*)
      ,TIMESTAMPDIFF(MINUTE,MIN(event_time), MAX(event_time) ) AS DIIF
FROM CTE2
GROUP BY userid,SM
