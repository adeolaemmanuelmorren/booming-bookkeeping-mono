import { readFile, writeFile } from 'node:fs/promises';

// Real-data workload benchmark, not a replacement for production source models.
// Preserved browser inputs only. Identity is pinned to a verified published batch.
const root = new URL('./', import.meta.url);
const identityVersion = 2497;
const cutoff = '2026-09-05 00:00:00';
const start = process.argv[2] ?? '2026-08-29';
const sample = Number(process.argv[3] ?? 16);
if (!/^2026-\d\d-\d\d$/.test(start) || ![1, 4, 16, 64].includes(sample)) throw Error('Invalid benchmark selection');

async function fields(table) {
  const text = await readFile(new URL(`../../restore/datasources/${table}.datasource`, import.meta.url), 'utf8');
  return new Set([...text.matchAll(/^    `([^`]+)`/gm)].map(match => match[1]));
}
function nullableString(columns, ...names) {
  const options = names.filter(name => columns.has(name)).map(name => `nullIf(trimBoth(ifNull(\`${name}\`, '')), '')`);
  return options.length ? `coalesce(${options.join(', ')}, '')` : "''";
}
const parameter = (url, key) => `extractURLParameter(${url}, '${key}')`;

async function pageSource(table, priority) {
  const columns = await fields(table);
  const value = (...names) => nullableString(columns, ...names);
  const url = value('url', 'context_page_url');
  const utm = key => `coalesce(nullIf(${value('context_attribution_utm_' + key, 'context_campaign_utm_' + key,
    'context_campaign_' + ({source:'source',medium:'medium',campaign:'name',content:'content',term:'term',id:'id'}[key]))}, ''), ${parameter(url, 'utm_' + key)})`;
  return `SELECT ${value('message_id','id')} AS event_id,
    coalesce(timestamp, sent_at, received_at) AS event_time,
    ${value('anonymous_id')} AS anonymous_id, lowerUTF8(${value('user_id')}) AS user_id,
    lowerUTF8(${value('context_traits_email')}) AS email,
    ${url} AS page_url, ${value('path','context_page_path')} AS page_path,
    ${value('referrer','context_page_referrer')} AS referrer,
    ${utm('source')} AS raw_source, ${utm('medium')} AS raw_medium,
    ${utm('campaign')} AS utm_campaign, ${utm('content')} AS utm_content,
    ${utm('term')} AS utm_term, ${utm('id')} AS utm_id,
    ${value('context_attribution_fbclid')} AS stored_fbclid,
    ${value('context_attribution_gclid')} AS stored_gclid,
    ${value('context_attribution_gbraid')} AS stored_gbraid,
    ${value('context_attribution_wbraid')} AS stored_wbraid,
    ${priority} AS source_priority, coalesce(received_at, sent_at, timestamp) AS revision
  FROM ${table}
  WHERE event_id != '' AND event_time < toDateTime64('${cutoff}',6,'UTC')`;
}

async function conversionSource(table, kind, priority) {
  const columns = await fields(table);
  const value = (...names) => nullableString(columns, ...names);
  const amount = columns.has('total') ? 'toFloat64(ifNull(total,0))' : 'toFloat64(0)';
  return `SELECT concat('${kind}:', ${value('message_id','id','event_id')}) AS conversion_id,
    coalesce(timestamp,sent_at,received_at) AS conversion_time,
    '${kind}' AS conversion_type, ${value('anonymous_id')} AS anonymous_id,
    lowerUTF8(${value('user_id')}) AS user_id,
    lowerUTF8(${value('email','context_traits_email')}) AS email,
    ${amount} AS amount, upperUTF8(${value('currency')}) AS currency,
    ${priority} AS source_priority, coalesce(received_at,sent_at,timestamp) AS revision
  FROM ${table}
  WHERE conversion_time < toDateTime64('${cutoff}',6,'UTC')
    AND conversion_time >= toDateTime64('${start} 00:00:00',6,'America/Los_Angeles')
    AND cityHash64(conversion_id) % ${sample} = 0`;
}

const pageInputs = await Promise.all([
  pageSource('v1_history_boom_domains_pages',1),
  pageSource('v1_history_jitsu_data_pages',2),
]);
const conversionInputs = await Promise.all([
  conversionSource('v1_history_boom_domains_form_submitted','client_form',1),
  conversionSource('v1_history_jitsu_data_form_submitted','client_form',2),
  conversionSource('v1_history_boom_domains_order_completed','client_payment',1),
  conversionSource('v1_history_jitsu_data_order_completed','client_payment',2),
]);
const ctes = `WITH
conversion_inputs AS (${conversionInputs.join('\nUNION ALL\n')}),
conversions AS (
  SELECT * EXCEPT source_priority,revision FROM conversion_inputs
  QUALIFY row_number() OVER (PARTITION BY conversion_id ORDER BY source_priority DESC,revision DESC)=1
),
conversion_identifiers AS (
  SELECT conversion_id AS fact_id,
    arrayJoin([tuple(concat('email:',email),1),tuple(concat('user_id:',user_id),2),tuple(concat('anonymous_id:',anonymous_id),3)]) AS candidate
  FROM conversions
),
identity_current AS (
  SELECT concat(id_type, ':', id_value_norm) AS identity_key,
    argMax(tuple(profile_id,action),tuple(
      if(startsWith(snapshot_id,'identity-worker:'),toUInt64OrZero(splitByChar(':',snapshot_id)[2]),toUInt64(0)),
      exported_at,row_hash)) AS head
  FROM identifiers
  WHERE tenant_id='boom' AND id_type IN ('anonymous_id','user_id','email')
    AND identity_key IN (SELECT candidate.1 FROM conversion_identifiers)
    AND (snapshot_id='identity-simple-20260907-v1' OR snapshot_id IN (
      SELECT concat('identity-worker:',toString(batch_version),':',batch_id)
      FROM v1_identity_commits WHERE tenant_id='boom' AND batch_version<=${identityVersion}
    ))
  GROUP BY identity_key
),
identities AS (SELECT identity_key, head.1 AS profile_id FROM identity_current WHERE head.2!='removed'),
conversion_profiles AS (
  SELECT f.fact_id,argMin(i.profile_id,if(i.profile_id='',100,f.candidate.2)) AS profile_id
  FROM conversion_identifiers f LEFT JOIN identities i ON f.candidate.1=i.identity_key
  GROUP BY f.fact_id
),
relevant_profiles AS (SELECT DISTINCT profile_id FROM conversion_profiles WHERE profile_id!=''),
touch_lookup_keys AS (
  SELECT DISTINCT concat(id_type,':',id_value_norm) AS identity_key
  FROM identifiers WHERE tenant_id='boom' AND id_type IN ('anonymous_id','user_id','email')
    AND profile_id IN (SELECT profile_id FROM relevant_profiles)
),
touch_identity_current AS (
  SELECT concat(id_type,':',id_value_norm) AS identity_key,
    argMax(tuple(profile_id,action),tuple(
      if(startsWith(snapshot_id,'identity-worker:'),toUInt64OrZero(splitByChar(':',snapshot_id)[2]),toUInt64(0)),
      exported_at,row_hash)) AS head
  FROM identifiers WHERE tenant_id='boom' AND identity_key IN (SELECT identity_key FROM touch_lookup_keys)
    AND (snapshot_id='identity-simple-20260907-v1' OR snapshot_id IN (
      SELECT concat('identity-worker:',toString(batch_version),':',batch_id)
      FROM v1_identity_commits WHERE tenant_id='boom' AND batch_version<=${identityVersion}
    )) GROUP BY identity_key
),
touch_identities AS (
  SELECT identity_key,head.1 AS profile_id FROM touch_identity_current WHERE head.2!='removed'
),
page_inputs_all AS (${pageInputs.join('\nUNION ALL\n')}),
relevant_visitors AS (
  SELECT DISTINCT coalesce(nullIf(anonymous_id,''),nullIf(user_id,''),event_id) AS visitor_key
  FROM page_inputs_all
  WHERE concat('anonymous_id:',anonymous_id) IN (SELECT identity_key FROM touch_identities)
    OR concat('user_id:',user_id) IN (SELECT identity_key FROM touch_identities)
    OR concat('email:',email) IN (SELECT identity_key FROM touch_identities)
),
page_inputs AS (
  SELECT * FROM page_inputs_all
  WHERE coalesce(nullIf(anonymous_id,''),nullIf(user_id,''),event_id) IN (SELECT visitor_key FROM relevant_visitors)
),
pages AS (
  SELECT * EXCEPT source_priority,revision FROM page_inputs
  QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY source_priority DESC,revision DESC)=1
),
ordered_pages AS (
  SELECT *, coalesce(nullIf(anonymous_id,''),nullIf(user_id,''),event_id) AS visitor_key,
    lagInFrame(toNullable(event_time),1,NULL) OVER (
      PARTITION BY visitor_key ORDER BY event_time,event_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS previous_time
  FROM pages
),
numbered_pages AS (
  SELECT *, sum(toUInt64(previous_time IS NULL OR
    toUnixTimestamp64Micro(event_time)-toUnixTimestamp64Micro(previous_time)>=1860000000)) OVER (
      PARTITION BY visitor_key ORDER BY event_time,event_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_number
  FROM ordered_pages
),
session_first AS (
  SELECT *, hex(MD5(concat(visitor_key,'|',toString(session_number)))) AS session_id
  FROM numbered_pages
  QUALIFY row_number() OVER (PARTITION BY visitor_key,session_number ORDER BY event_time,event_id)=1
),
session_sources AS (
  SELECT *, lowerUTF8(domain(referrer)) AS referrer_host,
    multiIf(extractURLParameter(page_url,'fbclid')!='' OR stored_fbclid!='','meta',
      extractURLParameter(page_url,'gclid')!='' OR stored_gclid!='' OR stored_gbraid!='' OR stored_wbraid!='','google',
      lowerUTF8(raw_source) IN ('fb','ig','facebook','instagram','meta','an','th','msg'),'meta',
      lowerUTF8(raw_source) IN ('google','adwords'),'google',lowerUTF8(raw_source)) AS normalized_source
  FROM session_first
),
session_dimensions AS (
  SELECT *,
    coalesce(nullIf(normalized_source,''),
      if(referrer_host='' OR match(referrer_host,'(^|\\\\.)(boomingbookkeeping\\\\.com|boomingbookkeeper\\\\.com|thebookkeepingchallenge\\\\.com|keyboardrichchallenge\\\\.com|keyboardrich\\\\.com)$'),'direct',referrer_host)) AS source,
    multiIf(source='meta',coalesce(nullIf(utm_campaign,''),utm_id),
      source='google',extractURLParameter(page_url,'gc_id'),'') AS campaign_id,
    multiIf(source='meta',coalesce(nullIf(utm_term,''),extractURLParameter(page_url,'fbc_id')),
      source='google',extractURLParameter(page_url,'h_ga_id'),'') AS adset_id,
    multiIf(source='meta',coalesce(nullIf(utm_content,''),extractURLParameter(page_url,'h_ad_id')),
      source='google',extractURLParameter(page_url,'h_ad_id'),'') AS ad_id,
    multiIf(source='meta' AND campaign_id!='' AND ad_id!='' AND
      (lowerUTF8(extractURLParameter(page_url,'utm_medium')) IN ('paid','paid_social','cpc','ppc') OR extractURLParameter(page_url,'fbclid')!=''),'paid',
      source='google' AND campaign_id!='' AND
      (lowerUTF8(raw_medium) IN ('paid','paid_social','cpc','ppc') OR stored_gclid!='' OR stored_gbraid!='' OR stored_wbraid!='' OR extractURLParameter(page_url,'gclid')!=''),'cpc',
      source='meta','social',raw_medium!='',lowerUTF8(raw_medium),source='direct','none','referral') AS medium
  FROM session_sources
),
fact_identifiers AS (
  SELECT concat('touch:',session_id) AS fact_id,
    arrayJoin([tuple(concat('anonymous_id:',anonymous_id),1),tuple(concat('user_id:',user_id),2),tuple(concat('email:',email),3)]) AS candidate
  FROM session_dimensions
),
fact_profiles AS (
  SELECT f.fact_id, argMin(i.profile_id,if(i.profile_id='',100,f.candidate.2)) AS profile_id
  FROM fact_identifiers f LEFT JOIN touch_identities i ON f.candidate.1=i.identity_key
  GROUP BY f.fact_id
),
touchpoints AS (
  SELECT s.*,hex(MD5(concat(session_id,'|website_session'))) AS touchpoint_id,p.profile_id
  FROM session_dimensions s INNER JOIN fact_profiles p ON concat('touch:',s.session_id)=p.fact_id
  WHERE p.profile_id!=''
),
resolved_conversions AS (
  SELECT c.*,p.profile_id FROM conversions c LEFT JOIN conversion_profiles p ON c.conversion_id=p.fact_id
),
joined AS (
  SELECT c.conversion_id,c.conversion_type,c.amount,c.currency,c.profile_id,
    c.conversion_time,t.touchpoint_id,t.session_id,
    if(t.touchpoint_id='',c.conversion_time,t.event_time) AS click_time,
    if(t.touchpoint_id='','offline',t.source) AS source,
    if(t.touchpoint_id='','offline',t.medium) AS medium,
    t.campaign_id,t.adset_id,t.ad_id,t.utm_campaign AS campaign_name,
    'unknown' AS adset_name,'unknown' AS ad_name,t.utm_content,t.utm_term,
    if(t.touchpoint_id='','offline',domain(t.page_url)) AS landing_page_host,
    if(t.touchpoint_id='','offline',t.page_path) AS landing_page_path,
    source='direct' AND medium='none' AS is_direct_touch,
    source IN ('meta','google') AND medium IN ('cpc','paid','paid_social')
      AND t.campaign_id!='' AND t.ad_id!='' AS is_paid_ad_touch
  FROM resolved_conversions c LEFT JOIN touchpoints t
    ON c.profile_id=t.profile_id AND t.event_time<=c.conversion_time
),
ordered AS (
  SELECT *,row_number() OVER (PARTITION BY conversion_id ORDER BY click_time,touchpoint_id) AS touchpoint_number
  FROM joined
),
eligible AS (SELECT *,NOT(is_direct_touch AND touchpoint_number>1) AS eligible FROM ordered),
ranked AS (
  SELECT *,countIf(eligible) OVER (PARTITION BY conversion_id) AS attribution_touchpoints,
    countIf(eligible) OVER (PARTITION BY conversion_id ORDER BY click_time,touchpoint_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS attribution_touchpoint_number
  FROM eligible
),
weighted AS (
  SELECT *,multiIf(NOT eligible,0.,attribution_touchpoints=1,1.,attribution_touchpoints=2,0.5,
    attribution_touchpoint_number=1 OR attribution_touchpoint_number=attribution_touchpoints,0.4,
    0.2/greatest(attribution_touchpoints-2,1)) AS multi_touch_weight,
    toFloat64(eligible AND attribution_touchpoint_number=1) AS first_touch_weight,
    toFloat64(eligible AND attribution_touchpoint_number=attribution_touchpoints) AS last_touch_weight
  FROM ranked
)`;

const settings = 'SETTINGS join_use_nulls=0, max_execution_time=30, max_memory_usage=1073741824, max_threads=2';
const metrics = {
  form_submissions_client_side: "toFloat64(conversion_type='client_form')",
  order_completed_client_side: "toFloat64(conversion_type='client_payment')",
  payments_client_side: "toFloat64(conversion_type='client_payment')",
  revenue_client_side: 'amount',
};
const aggregates = Object.entries(metrics).flatMap(([name,expr]) => ['multi','first','last'].flatMap(method => [
  `sum((${expr}) * ${method}_touch_weight) AS ${name}_${method}`,
  `sum((${expr}) * ${method}_touch_weight * is_paid_ad_touch) AS ${name}_paid_${method}`,
]));
const groupColumns = ['click_date','source','medium','campaign_name','adset_name','ad_name','campaign_id','adset_id','ad_id','utm_content','utm_term','landing_page_host','landing_page_path','currency'];
const query = `${ctes},\nreport AS (
  SELECT toDate(click_time,'America/Los_Angeles') AS click_date,
    ${groupColumns.slice(1).join(',')},uniqExactIf(session_id,session_id!='') AS sessions,
    count() AS pair_rows,uniqExact(conversion_id) AS conversions_in_group,
    ${aggregates.join(',\n    ')}
  FROM weighted
  WHERE click_time>=toDateTime64('${start} 00:00:00',6,'America/Los_Angeles')
  GROUP BY ${groupColumns.join(',')}
)
SELECT count() AS report_groups,sum(pair_rows) AS pair_rows,
  sum(sessions) AS sessions_across_groups,
  ${aggregates.map(item => item.split(' AS ')[1]).map(name => `sum(${name}) AS ${name}`).join(',\n  ')}
FROM report
${settings}\n`;
const stem = `report-${start}-sample-${sample}`;
await writeFile(new URL(`${stem}.sql`,root),query);
await writeFile(new URL('input-counts.sql',root),`${ctes}\nSELECT 'conversions' AS kind, conversion_type AS detail,count() AS rows,uniqExactIf(profile_id,profile_id!='') AS profiles,countIf(profile_id='') AS unresolved FROM resolved_conversions GROUP BY conversion_type UNION ALL SELECT 'touchpoints','',count(),uniqExact(profile_id),0 FROM touchpoints\n${settings}\n`);
await writeFile(new URL('weight-check.sql',root),`${ctes}, checks AS (SELECT conversion_id,sum(first_touch_weight) AS ft,sum(last_touch_weight) AS lt,sum(multi_touch_weight) AS mt FROM weighted GROUP BY conversion_id) SELECT count() AS conversions,countIf(abs(ft-1)>1e-9 OR abs(lt-1)>1e-9 OR abs(mt-1)>1e-9) AS invalid_weight_totals FROM checks\n${settings}\n`);
console.log(JSON.stringify({sql:stem+'.sql',identityVersion,cutoff,start,sample,metrics:Object.keys(metrics)}));
