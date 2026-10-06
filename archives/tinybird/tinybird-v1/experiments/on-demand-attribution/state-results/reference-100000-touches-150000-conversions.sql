SELECT source,campaign_id,ad_id,count() AS conversions_ft,sum(amount_cents) AS revenue_ft_cents
    FROM (SELECT *,row_number() OVER (PARTITION BY conversion_id ORDER BY session_time,touchpoint_id) AS rank
    FROM (SELECT c.conversion_id,c.amount_cents,t.touchpoint_id,t.session_time,
    if(t.touchpoint_id=0,'offline',t.source) AS source,t.campaign_id,t.ad_id
    FROM (SELECT facts.*,identities.profile_id FROM (SELECT number+1 AS conversion_id,intDiv(number,300)*2+2 AS identity_key,
    1788246000+toUInt64((number%3+1)*200/3)*3600 AS conversion_time,
    multiIf(number%3=0,10000,number%3=1,5000,0) AS amount_cents FROM numbers(150000)) facts INNER JOIN (SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(1000)) identities
    ON facts.identity_key=identities.identity_key) c LEFT JOIN (SELECT facts.*,identities.profile_id FROM (SELECT number+1 AS touchpoint_id,intDiv(number,200)*2+1 AS identity_key,
    1788246000+toUInt64(number%200)*3600 AS touch_time,touch_time AS session_time,
    if(number%200 IN (0,7),'direct','google') AS source,
    if(source='direct','none','cpc') AS medium,number%4+1 AS campaign_id,number%4+1 AS ad_id
    FROM numbers(100000)) facts INNER JOIN (SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(1000)) identities
    ON facts.identity_key=identities.identity_key) t
      ON c.profile_id=t.profile_id AND t.touch_time<=c.conversion_time)) WHERE rank=1 GROUP BY source,campaign_id,ad_id ORDER BY source,campaign_id,ad_id SETTINGS join_use_nulls=0, max_execution_time=20, max_memory_usage=536870912