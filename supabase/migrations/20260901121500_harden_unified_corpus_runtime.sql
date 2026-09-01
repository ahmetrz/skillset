begin;

create index if not exists unified_synthesis_candidates_v1_canonical_hash_idx
  on skillset.unified_synthesis_candidates_v1(canonical_hash);

revoke all on function public.skillset_unified_eval_enqueue_v1(text,integer)
  from public,anon,authenticated;
grant execute on function public.skillset_unified_eval_enqueue_v1(text,integer)
  to service_role;

commit;
