create index if not exists evaluation_v31h_cache_delta_lookup_idx
  on skillset.evaluation_v31h_cache
  (source_content_hash, implementation_rev, projected_content_hash);

create index if not exists sdlc_projection_hash_v52_eval_ready_idx
  on skillset.sdlc_projection_hash_v52
  (source_content_hash, projected_content_hash)
  where transformer_version = 'sdlc-projection-v5.2'
    and decision = 'keep'
    and residual_model_risks = 0
    and projected_content_gzip is not null
    and projected_content_hash is not null;

create index if not exists skills_content_hash_id_idx
  on skillset.skills(content_hash, id)
  where content_hash is not null;

create or replace function public.skillset_eval_v31_canonical_start_delta(
  p_limit integer default 5000
)
returns table(run_id uuid, expected_items integer, corpus_fingerprint text)
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
declare
  v_run uuid;
  v_expected integer;
  v_fp text;
  v_cutoff timestamptz := now();
  v_locked boolean;
  v_has_candidate boolean;
begin
  v_locked := pg_try_advisory_xact_lock(hashtext('skillset_eval_v31h_singleton'));
  if not v_locked then
    return query
    select r.id, r.expected_items, r.corpus_fingerprint
    from skillset.evaluation_runs r
    where r.implementation_rev = '3.1h' and r.status = 'running'
    order by r.started_at desc
    limit 1;
    return;
  end if;

  if exists (
    select 1
    from skillset.evaluation_runs
    where implementation_rev = '3.1h' and status = 'running'
  ) then
    return query
    select r.id, r.expected_items, r.corpus_fingerprint
    from skillset.evaluation_runs r
    where r.implementation_rev = '3.1h' and r.status = 'running'
    order by r.started_at desc
    limit 1;
    return;
  end if;

  select exists (
    select 1
    from skillset.sdlc_projection_hash_v52 h
    where h.transformer_version = 'sdlc-projection-v5.2'
      and h.decision = 'keep'
      and h.residual_model_risks = 0
      and h.projected_content_gzip is not null
      and h.projected_content_hash is not null
      and not exists (
        select 1
        from skillset.evaluation_v31h_cache cache
        where cache.source_content_hash = h.source_content_hash
          and cache.implementation_rev = '3.1h'
          and cache.projected_content_hash = h.projected_content_hash
      )
  ) into v_has_candidate;

  if not v_has_candidate then
    return;
  end if;

  insert into skillset.evaluation_runs(
    rubric_version, implementation_rev, status, expected_items,
    corpus_fingerprint, content_created_before, detail, started_at
  )
  values(
    '3.1', '3.1h', 'running', 0, 'pending', v_cutoff,
    jsonb_build_object(
      'non_destructive', true, 'delta', true,
      'source_table', 'skillset.sdlc_projection_hash_v52',
      'cache_table', 'skillset.evaluation_v31h_cache',
      'unit', 'source_content_hash',
      'projection_transformer', 'sdlc-projection-v5.2',
      'precision_first', true
    ),
    now()
  )
  returning id into v_run;

  with candidates as materialized (
    select h.source_content_hash, h.projected_content_hash
    from skillset.sdlc_projection_hash_v52 h
    where h.transformer_version = 'sdlc-projection-v5.2'
      and h.decision = 'keep'
      and h.residual_model_risks = 0
      and h.projected_content_gzip is not null
      and h.projected_content_hash is not null
      and not exists (
        select 1
        from skillset.evaluation_v31h_cache cache
        where cache.source_content_hash = h.source_content_hash
          and cache.implementation_rev = '3.1h'
          and cache.projected_content_hash = h.projected_content_hash
      )
    order by h.source_content_hash
    limit greatest(1, least(p_limit, 5000))
  )
  insert into skillset.evaluation_v31_canonical(
    run_id, source_content_hash, representative_skill_id,
    projected_content_hash, status
  )
  select v_run, c.source_content_hash, s.id, c.projected_content_hash, 'pending'
  from candidates c
  join lateral (
    select x.id
    from skillset.skills x
    where x.content_hash = c.source_content_hash
    order by x.id
    limit 1
  ) s on true;

  select
    count(*),
    md5(string_agg(
      e.source_content_hash || ':' || e.projected_content_hash,
      ',' order by e.source_content_hash
    ))
  into v_expected, v_fp
  from skillset.evaluation_v31_canonical e
  where e.run_id = v_run;

  if v_expected = 0 then
    delete from skillset.evaluation_runs where id = v_run;
    return;
  end if;

  update skillset.evaluation_runs
  set expected_items = v_expected,
      corpus_fingerprint = v_fp,
      detail = detail || jsonb_build_object('delta_items', v_expected),
      updated_at = now()
  where id = v_run;

  update skillset.evaluation_canonical_control
  set enabled = true,
      allowed_run_id = v_run,
      run_tokens = 0,
      max_batch = 50,
      updated_at = now()
  where singleton = true;

  return query select v_run, v_expected, v_fp;
end
$function$;
