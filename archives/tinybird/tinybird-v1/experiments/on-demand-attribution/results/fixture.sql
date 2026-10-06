SELECT conversion_id,touchpoint_id,mt,ft,lt
  FROM (SELECT *, multiIf(NOT eligible,0.,eligible_count=1,1.,eligible_count=2,0.5,
    eligible_position=1 OR eligible_position=eligible_count,0.4,0.2/greatest(eligible_count-2,1)) AS mt,
    toFloat64(eligible AND eligible_position=1) AS ft,
    toFloat64(eligible AND eligible_position=eligible_count) AS lt FROM (SELECT *, countIf(eligible) OVER (PARTITION BY conversion_id) AS eligible_count,
    countIf(eligible) OVER (PARTITION BY conversion_id ORDER BY click_time,touchpoint_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS eligible_position FROM (SELECT *, NOT (is_direct AND position>1) AS eligible FROM (SELECT *, row_number() OVER
    (PARTITION BY conversion_id ORDER BY click_time,touchpoint_id) AS position FROM (SELECT c.conversion_id, c.amount_cents, t.touchpoint_id,
    if(t.touchpoint_id=0,c.conversion_time,t.session_time) AS click_time,
    if(t.touchpoint_id=0,'offline',t.source) AS source,
    t.campaign_id, t.ad_id,
    t.source='direct' AND t.medium='none' AS is_direct,
    t.source IN ('google','meta') AND t.medium IN ('cpc','paid','paid_social')
      AND t.campaign_id>0 AND t.ad_id>0 AS is_paid
    FROM (SELECT facts.*, identities.profile_id FROM (SELECT item.1 AS conversion_id,item.2 AS identity_key,item.3 AS conversion_time,item.4 AS amount_cents
    FROM (SELECT arrayJoin([tuple(1,200,1789974000,10000),tuple(2,201,1789628400,8000),tuple(3,202,1789974000,4000),tuple(4,203,1789369200,1000),tuple(5,204,1789455600,2000),tuple(6,205,1789369200,6000),tuple(7,202,1790060400,0),tuple(8,200,1789282800,5000)]) AS item)) facts
    INNER JOIN (SELECT item.1 AS identity_key,item.2 AS profile_id
    FROM (SELECT arrayJoin([tuple(100,1),tuple(200,1),tuple(101,2),tuple(201,2),tuple(102,3),tuple(202,3),tuple(103,4),tuple(203,4),tuple(104,5),tuple(204,5),tuple(105,6),tuple(205,6)]) AS item)) identities ON facts.identity_key=identities.identity_key) c
    LEFT JOIN (SELECT facts.*, identities.profile_id FROM (SELECT item.1 AS touchpoint_id,item.2 AS identity_key,item.3 AS touch_time,item.4 AS session_time,item.5 AS source,item.6 AS medium,item.7 AS campaign_id,item.8 AS ad_id
    FROM (SELECT arrayJoin([tuple(1,100,1788246000,1788246000,'direct','none',0,0),tuple(2,100,1789196400,1789196400,'google','cpc',1,1),tuple(3,100,1789282800,1789282800,'direct','none',0,0),tuple(4,100,1789628400,1789628400,'meta','paid',2,2),tuple(5,100,1790146800,1790146800,'google','cpc',3,3),tuple(6,101,1789196400,1789196400,'google','cpc',1,1),tuple(7,101,1789282800,1789282800,'meta','paid',2,2),tuple(8,102,1789282800,1789282800,'google','cpc',1,1),tuple(9,104,1790406000,1790406000,'google','cpc',1,1),tuple(10,105,1789282800,1789282800,'google','cpc',1,1),tuple(11,105,1789282800,1789282800,'meta','paid',2,2)]) AS item)) facts
    INNER JOIN (SELECT item.1 AS identity_key,item.2 AS profile_id
    FROM (SELECT arrayJoin([tuple(100,1),tuple(200,1),tuple(101,2),tuple(201,2),tuple(102,3),tuple(202,3),tuple(103,4),tuple(203,4),tuple(104,5),tuple(204,5),tuple(105,6),tuple(205,6)]) AS item)) identities ON facts.identity_key=identities.identity_key) t
      ON c.profile_id=t.profile_id AND t.touch_time<=c.conversion_time))))) ORDER BY conversion_id,touchpoint_id SETTINGS join_use_nulls=0, max_execution_time=20, max_memory_usage=536870912