begin;

-- The final B2 source rescue arrived after the original projection-size scan.
-- Route every still-open gzip above the locally measured 1.5 MB safe ceiling
-- through the lossless projection splitter before another Edge attempt.
update skillset.gitskills_projection_queue_v1 q
set status='rescue_pending',
    claimed_at=null,
    error=coalesce(q.error,'oversized_projection_requires_lossless_split'),
    updated_at=now()
from storage.objects o
where o.bucket_id='skill-discovery-v1'
  and o.name=q.input_path
  and q.input_path like 'gitskills/%'
  and q.status in ('pending','processing','error')
  and coalesce((o.metadata->>'size')::bigint,0)>1500000;

commit;
