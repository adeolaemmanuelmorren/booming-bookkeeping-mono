export const settings = 'SETTINGS join_use_nulls=0, max_execution_time=20, max_memory_usage=536870912';
export const epoch = 1788246000;

export function literalRows(rows, columns) {
  const tuples = rows.map(row => `tuple(${row.map(value => typeof value === 'string' ? `'${value}'` : value).join(',')})`);
  return `SELECT ${columns.map((column,i) => `item.${i+1} AS ${column}`).join(',')}
    FROM (SELECT arrayJoin([${tuples.join(',')}]) AS item)`;
}

function resolve(source, mapping) {
  return `SELECT facts.*,identities.profile_id FROM (${source}) facts
    INNER JOIN (${mapping}) identities ON facts.identity_key=identities.identity_key`;
}

// The supplied mapping is an externally computed, complete current identity version.
// ASOF selects a timestamp boundary, not one arbitrary touch at that timestamp.
export function journeyQuery(touches, conversions, mapping, { start, end, byTime=false, useArrays=false }={}) {
  const resolvedTouches = resolve(touches,mapping);
  const boundaries = `SELECT profile_id,touch_time FROM (${resolvedTouches}) GROUP BY profile_id,touch_time`;
  const matched = `SELECT c.profile_id,c.conversion_time,c.amount_cents,b.touch_time AS boundary
    FROM (${resolve(conversions,mapping)}) c ASOF LEFT JOIN (${boundaries}) b
      ON c.profile_id=b.profile_id AND c.conversion_time>=b.touch_time`;
  // Offline rows must keep their conversion time for click-date reporting.
  const grouped = `SELECT profile_id,boundary,if(boundary=0,conversion_time,0) AS offline_time,
    count() AS conversion_count,sum(toInt64(amount_cents)) AS amount_cents
    FROM (${matched}) GROUP BY profile_id,boundary,offline_time`;
  if (!useArrays) return groupedJoinQuery(grouped,resolvedTouches,{start,end,byTime});
  const journeys = `SELECT profile_id,
    arraySort(x -> (x.1,x.2),groupArray(tuple(toUInt64(session_time),toUInt64(touchpoint_id),
      toUInt64(touch_time),source,medium,toUInt64(campaign_id),toUInt64(ad_id)))) AS full_journey
    FROM (${resolvedTouches}) GROUP BY profile_id`;
  const selected = `SELECT g.*,arrayFilter(x -> x.3<=g.boundary,j.full_journey) AS prior_touches
    FROM (${grouped}) g LEFT JOIN (${journeys}) j ON g.profile_id=j.profile_id`;
  const filled = `SELECT *,if(empty(prior_touches),
    [tuple(toUInt64(offline_time),toUInt64(0),toUInt64(0),'offline','offline',toUInt64(0),toUInt64(0))],
    prior_touches) AS journey FROM (${selected})`;
  // Retain excluded direct touches with zero credit, matching existing output groups.
  const eligibility = `SELECT *,arrayMap((x,i) -> toUInt64(i=1 OR NOT (x.4='direct' AND x.5='none')),
    journey,arrayEnumerate(journey)) AS eligible FROM (${filled})`;
  const expanded = `SELECT conversion_count,amount_cents,arraySum(eligible) AS eligible_count,
    arrayJoin(arrayZip(journey,eligible,arrayCumSum(eligible))) AS item FROM (${eligibility})`;
  const weighted = `SELECT conversion_count,amount_cents,item.1.1 AS click_time,
    item.1.4 AS source,item.1.6 AS campaign_id,item.1.7 AS ad_id,
    multiIf(item.2=0,0.,eligible_count=1,1.,eligible_count=2,0.5,
      item.3=1 OR item.3=eligible_count,0.4,0.2/greatest(eligible_count-2,1)) AS mt,
    toFloat64(item.2=1 AND item.3=1) AS ft,
    toFloat64(item.2=1 AND item.3=eligible_count) AS lt FROM (${expanded})`;
  const dimensions = `${byTime ? 'click_time,' : ''}source,campaign_id,ad_id`;
  const filter = start===undefined ? '' : `WHERE click_time>=${start} AND click_time<${end}`;
  return `SELECT ${dimensions},sum(conversion_count*mt) AS conversions_mt,
    sum(conversion_count*ft) AS conversions_ft,sum(conversion_count*lt) AS conversions_lt,
    sum(amount_cents*mt) AS revenue_cents FROM (${weighted}) ${filter}
    GROUP BY ${dimensions} ORDER BY ${dimensions} ${settings}`;
}

function groupedJoinQuery(grouped,touches,{start,end,byTime}) {
  const joined = `SELECT g.profile_id,g.boundary,g.offline_time,g.conversion_count,g.amount_cents,
    t.touchpoint_id,if(t.touchpoint_id=0,g.offline_time,t.session_time) AS click_time,
    if(t.touchpoint_id=0,'offline',t.source) AS source,t.campaign_id,t.ad_id,
    t.source='direct' AND t.medium='none' AS is_direct FROM (${grouped}) g
    LEFT JOIN (${touches}) t ON g.profile_id=t.profile_id AND t.touch_time<=g.boundary`;
  const partition = 'profile_id,boundary,offline_time';
  const ordered = `SELECT *,row_number() OVER (PARTITION BY ${partition}
    ORDER BY click_time,touchpoint_id) AS position FROM (${joined})`;
  const eligible = `SELECT *,NOT (is_direct AND position>1) AS eligible FROM (${ordered})`;
  const ranked = `SELECT *,countIf(eligible) OVER (PARTITION BY ${partition}) AS eligible_count,
    countIf(eligible) OVER (PARTITION BY ${partition} ORDER BY click_time,touchpoint_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS eligible_position FROM (${eligible})`;
  const weighted = `SELECT *,multiIf(NOT eligible,0.,eligible_count=1,1.,eligible_count=2,0.5,
    eligible_position=1 OR eligible_position=eligible_count,0.4,0.2/greatest(eligible_count-2,1)) AS mt,
    toFloat64(eligible AND eligible_position=1) AS ft,
    toFloat64(eligible AND eligible_position=eligible_count) AS lt FROM (${ranked})`;
  const dimensions = `${byTime?'click_time,':''}source,campaign_id,ad_id`;
  const filter = start===undefined?'':`WHERE click_time>=${start} AND click_time<${end}`;
  return `SELECT ${dimensions},sum(conversion_count*mt) AS conversions_mt,
    sum(conversion_count*ft) AS conversions_ft,sum(conversion_count*lt) AS conversions_lt,
    sum(amount_cents*mt) AS revenue_cents FROM (${weighted}) ${filter}
    GROUP BY ${dimensions} ORDER BY ${dimensions} ${settings}`;
}

export function generatedInputs(total, journeyLength, conversionsPerProfile=3, uniqueBoundaries=false) {
  const profiles = total/journeyLength;
  if (!Number.isInteger(profiles)) throw new Error('Whole profiles required');
  const mapping = `SELECT number+1 AS identity_key,intDiv(number,2)+1 AS profile_id FROM numbers(${profiles*2})`;
  const touches = `SELECT number+1 AS touchpoint_id,intDiv(number,${journeyLength})*2+1 AS identity_key,
    ${epoch}+toUInt64(number%${journeyLength})*3600 AS touch_time,touch_time AS session_time,
    if(number%${journeyLength} IN (0,7),'direct','google') AS source,
    if(source='direct','none','cpc') AS medium,number%4+1 AS campaign_id,number%4+1 AS ad_id
    FROM numbers(${total})`;
  const hour = uniqueBoundaries ? `number%${conversionsPerProfile}` : `(number%3+1)*${journeyLength}/3`;
  const conversions = `SELECT number+1 AS conversion_id,intDiv(number,${conversionsPerProfile})*2+2 AS identity_key,
    ${epoch}+toUInt64(${hour})*3600 AS conversion_time,
    multiIf(number%3=0,10000,number%3=1,5000,0) AS amount_cents FROM numbers(${profiles*conversionsPerProfile})`;
  return { touches,conversions,mapping,profiles,conversionCount:profiles*conversionsPerProfile };
}
