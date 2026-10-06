SELECT profile_id,argMinMerge(first_state) AS first FROM (
    SELECT profile_id,batch,argMinState(tuple(touchpoint_id,touch_time,session_time,source),
      tuple(session_time,touchpoint_id)) AS first_state FROM (SELECT item.1 AS batch,item.2 AS touchpoint_id,item.3 AS profile_id,item.4 AS touch_time,item.5 AS session_time,item.6 AS source
    FROM (SELECT arrayJoin([tuple(1,1,1,20,1,'google'),tuple(1,2,1,10,5,'meta')]) AS item))
    GROUP BY profile_id,batch) GROUP BY profile_id ORDER BY profile_id SETTINGS join_use_nulls=0, max_execution_time=20, max_memory_usage=536870912