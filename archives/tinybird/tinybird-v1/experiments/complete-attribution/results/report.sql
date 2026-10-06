SELECT if(item.1='','integrity','data') AS kind,item.1 AS group_key,
      sumForEach(arrayMap(value -> toFloat64(value),item.2)) AS metrics,
      length(groupUniqArrayArray(item.3)) AS sessions,sum(invalid) AS invalid,
      max(snapshot_version) AS version,max(conflicts) AS conflicts FROM
      (SELECT snapshot_version,conflicts,invalid,
        arrayJoin(arrayPushBack(arrayZip(groups,metrics,session_ids),tuple('',[],[]))) AS item FROM (SELECT e.snapshot_version,e.conflicts,e.chunk_id!='' AND (c.found=0 OR c.copies!=1) AS invalid,
    c.groups,c.metrics,c.session_ids FROM (SELECT profile_id,snapshot_version,conflicts,arrayJoin(arrayPushBack(ids,'')) AS chunk_id FROM (SELECT item.1 AS profile_id,argMax(item.2,version) AS ids,
    max(max(version)) OVER () AS snapshot_version,max(max(conflicts)) OVER () AS conflicts
    FROM (SELECT version,conflicts,
    arrayJoin(arrayZip(profiles,chunk_ids)) AS item FROM (SELECT *,count() OVER (PARTITION BY version)>1 AS conflicts FROM (SELECT DISTINCT version,transaction_id,profiles,chunk_ids FROM proof_attribution_commits_v2 WHERE scenario='complete-attribution:44886ecc-f157-42ae-b0c3-945e5d8ff012'))) GROUP BY profile_id)) e LEFT JOIN (SELECT *,toUInt8(1) AS found,count() OVER (PARTITION BY chunk_id) AS copies
    FROM (SELECT DISTINCT chunk_id,groups,metrics,sessions,session_ids FROM proof_attribution_chunks_v2 WHERE scenario='complete-attribution:44886ecc-f157-42ae-b0c3-945e5d8ff012')) c ON e.chunk_id=c.chunk_id))
      WHERE item.1='' OR (JSONExtractString(item.1,1)>='0000-00-00' AND JSONExtractString(item.1,1)<'9999-99-99')
      GROUP BY group_key
      SETTINGS max_execution_time=20,max_memory_usage=536870912