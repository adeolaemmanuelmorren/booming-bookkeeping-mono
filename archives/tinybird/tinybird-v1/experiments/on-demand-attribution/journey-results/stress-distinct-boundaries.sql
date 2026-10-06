SELECT source,campaign_id,ad_id,sum(conversion_count*mt) AS conversions_mt,
    sum(conversion_count*ft) AS conversions_ft,sum(conversion_count*lt) AS conversions_lt,
    sum(amount_cents*mt) AS revenue_cents FROM (SELECT *,multiIf(NOT eligible,0.,eligible_count=1,1.,eligible_count=2,0.5,
    eligible_position=1 OR eligible_position=eligible_count,0.4,0.2/greatest(eligible_count-2,1)) AS mt,
    toFloat64(eligible AND eligible_position=1) AS ft,
    toFloat64(eligible AND eligible_position=eligible_count) AS lt FROM (SELECT *,countIf(eligible) OVER (PARTITION BY profile_id,boundary,offline_time) AS eligible_count,
    countIf(eligible) OVER (PARTITION BY profile_id,boundary,offline_time ORDER BY click_time,touchpoint_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS eligible_position FROM (SELECT *,NOT (is_direct AND position>1) AS eligible FROM (SELECT *,row_number() OVER (PARTITION BY profile_id,boundary,offline_time
    ORDER BY click_time,touchpoint_id) AS position FROM (SELECT g.profile_id,g.boundary,g.offline_time,g.conversion_count,g.amount_cents,
    t.touchpoint_id,if(t.touchpoint_id=0,g.offline_time,t.session_time) AS click_time,
    if(t.touchpoint_id=0,'offline',t.source) AS source,t.campaign_id,t.ad_id,
    t.source='direct' AND t.medium='none' AS is_direct FROM (SELECT profile_id,boundary,if(boundary=0,conversion_time,0) AS offline_time,
    count() AS conversion_count,sum(toInt64(amount_cents)) AS amount_cents
    FROM (SELECT c.profile_id,c.conversion_time,c.amount_cents,b.touch_time AS boundary
    FROM (SELECT facts.*,identities.profile_id FROM (SELECT number+1 AS conversion_id,intDiv(number,200)*2+2 AS identity_key,
    1788246000+toUInt64(number%200)*3600 AS conversion_time,
    multiIf(number%3=0,10000,number%3=1,5000,0) AS amount_cents FROM numbers(100000)) facts
    INNER JOIN (SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(1000)) identities ON facts.identity_key=identities.identity_key) c ASOF LEFT JOIN (SELECT profile_id,touch_time FROM (SELECT facts.*,identities.profile_id FROM (SELECT number+1 AS touchpoint_id,intDiv(number,200)*2+1 AS identity_key,
    1788246000+toUInt64(number%200)*3600 AS touch_time,touch_time AS session_time,
    if(number%200 IN (0,7),'direct','google') AS source,
    if(source='direct','none','cpc') AS medium,number%4+1 AS campaign_id,number%4+1 AS ad_id
    FROM numbers(100000)) facts
    INNER JOIN (SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(1000)) identities ON facts.identity_key=identities.identity_key) GROUP BY profile_id,touch_time) b
      ON c.profile_id=b.profile_id AND c.conversion_time>=b.touch_time) GROUP BY profile_id,boundary,offline_time) g
    LEFT JOIN (SELECT facts.*,identities.profile_id FROM (SELECT number+1 AS touchpoint_id,intDiv(number,200)*2+1 AS identity_key,
    1788246000+toUInt64(number%200)*3600 AS touch_time,touch_time AS session_time,
    if(number%200 IN (0,7),'direct','google') AS source,
    if(source='direct','none','cpc') AS medium,number%4+1 AS campaign_id,number%4+1 AS ad_id
    FROM numbers(100000)) facts
    INNER JOIN (SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(1000)) identities ON facts.identity_key=identities.identity_key) t ON g.profile_id=t.profile_id AND t.touch_time<=g.boundary))))) 
    GROUP BY source,campaign_id,ad_id ORDER BY source,campaign_id,ad_id SETTINGS join_use_nulls=0, max_execution_time=20, max_memory_usage=536870912