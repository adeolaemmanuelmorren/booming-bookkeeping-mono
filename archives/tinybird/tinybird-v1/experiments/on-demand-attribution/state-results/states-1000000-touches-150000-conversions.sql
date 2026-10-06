SELECT source,campaign_id,ad_id,count() AS conversions_ft,sum(amount_cents) AS revenue_ft_cents
    FROM (SELECT c.amount_cents,if(s.first.1>0 AND s.first.2<=c.conversion_time,s.first.4,'offline') AS source,
    if(source='offline',0,s.first.5) AS campaign_id,if(source='offline',0,s.first.6) AS ad_id
    FROM (SELECT facts.*,identities.profile_id FROM (SELECT number+1 AS conversion_id,intDiv(number,3)*2+2 AS identity_key,
    1788246000+toUInt64((number%3+1)*20/3)*3600 AS conversion_time,
    multiIf(number%3=0,10000,number%3=1,5000,0) AS amount_cents FROM numbers(150000)) facts INNER JOIN (SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(100000)) identities
    ON facts.identity_key=identities.identity_key) c LEFT JOIN (SELECT profile_id,argMinMerge(first_state) AS first FROM (SELECT profile_id,intDiv(touchpoint_id,10000) AS batch,
    argMinState(tuple(touchpoint_id,touch_time,session_time,source,campaign_id,ad_id),
      tuple(session_time,touchpoint_id)) AS first_state
    FROM (SELECT facts.*,identities.profile_id FROM (SELECT number+1 AS touchpoint_id,intDiv(number,20)*2+1 AS identity_key,
    1788246000+toUInt64(number%20)*3600 AS touch_time,touch_time AS session_time,
    if(number%20 IN (0,7),'direct','google') AS source,
    if(source='direct','none','cpc') AS medium,number%4+1 AS campaign_id,number%4+1 AS ad_id
    FROM numbers(1000000)) facts INNER JOIN (SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(100000)) identities
    ON facts.identity_key=identities.identity_key) GROUP BY profile_id,batch) GROUP BY profile_id) s ON c.profile_id=s.profile_id) GROUP BY source,campaign_id,ad_id ORDER BY source,campaign_id,ad_id SETTINGS join_use_nulls=0, max_execution_time=20, max_memory_usage=536870912