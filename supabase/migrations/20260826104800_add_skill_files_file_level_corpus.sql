create table if not exists skillset.skill_files (
  file_id text primary key,
  source text not null,
  owner text,
  repo text,
  repo_path text not null,
  skill_name text,
  logical_skill_id text,
  source_url text,
  content_sha256 text,
  content_bytes integer,
  content_object_key text,
  compressed_bytes integer,
  storage_backend text,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  retrieved_at timestamptz,
  updated_at timestamptz not null default now(),
  unique(source, repo_path)
);
create index if not exists skill_files_source_idx on skillset.skill_files(source);
create index if not exists skill_files_hash_idx on skillset.skill_files(content_sha256);
create index if not exists skill_files_logical_idx on skillset.skill_files(logical_skill_id);

create or replace function public.skillset_upsert_repo_discovered_skill(
  p_source text,
  p_skill_name text,
  p_repo_path text,
  p_owner text,
  p_repo text,
  p_source_url text,
  p_sha text,
  p_raw_bytes integer,
  p_object_key text,
  p_gzip_bytes integer
) returns boolean
language plpgsql
security definer
set search_path to skillset, public
as $$
declare
  v_id text;
  v_logical_inserted boolean;
  v_file_inserted boolean;
  v_file_id text := p_source||'/@file/'||trim(both '/' from p_repo_path);
begin
  select s.id into v_id
  from skillset.skills s
  where s.source=p_source and lower(s.skill_name)=lower(p_skill_name)
  order by case when s.id=p_source||'/'||p_skill_name then 0 else 1 end,s.first_seen_at
  limit 1;
  if v_id is null then v_id:=p_source||'/@repo/'||trim(both '/' from p_repo_path); end if;

  insert into skillset.skills(id,source,skill_name,owner,repo,skill_url,source_url,installs,content_sha256,content_bytes,content_object_key,content_encoding,compressed_bytes,storage_backend,retrieval_status,first_seen_at,last_seen_at,retrieved_at,updated_at)
  values(v_id,p_source,p_skill_name,p_owner,p_repo,null,p_source_url,0,p_sha,p_raw_bytes,p_object_key,'gzip',p_gzip_bytes,'supabase-storage','ok',now(),now(),now(),now())
  on conflict(id) do update set content_sha256=excluded.content_sha256,content_bytes=excluded.content_bytes,content_object_key=excluded.content_object_key,content_encoding='gzip',compressed_bytes=excluded.compressed_bytes,storage_backend='supabase-storage',source_url=excluded.source_url,retrieval_status='ok',retrieval_error=null,last_seen_at=now(),retrieved_at=now(),updated_at=now()
  returning (xmax=0) into v_logical_inserted;

  insert into skillset.skill_files(file_id,source,owner,repo,repo_path,skill_name,logical_skill_id,source_url,content_sha256,content_bytes,content_object_key,compressed_bytes,storage_backend,first_seen_at,last_seen_at,retrieved_at,updated_at)
  values(v_file_id,p_source,p_owner,p_repo,p_repo_path,p_skill_name,v_id,p_source_url,p_sha,p_raw_bytes,p_object_key,p_gzip_bytes,'supabase-storage',now(),now(),now(),now())
  on conflict(source,repo_path) do update set skill_name=excluded.skill_name,logical_skill_id=excluded.logical_skill_id,source_url=excluded.source_url,content_sha256=excluded.content_sha256,content_bytes=excluded.content_bytes,content_object_key=excluded.content_object_key,compressed_bytes=excluded.compressed_bytes,storage_backend='supabase-storage',last_seen_at=now(),retrieved_at=now(),updated_at=now()
  returning (xmax=0) into v_file_inserted;
  return coalesce(v_file_inserted,false);
end;
$$;

insert into skillset.skill_files(file_id,source,owner,repo,repo_path,skill_name,logical_skill_id,source_url,content_sha256,content_bytes,content_object_key,compressed_bytes,storage_backend,first_seen_at,last_seen_at,retrieved_at,updated_at)
select s.source||'/@file/'||regexp_replace(s.source_url,'^https://github.com/[^/]+/[^/]+/blob/[^/]+/','')::text,
       s.source,s.owner,s.repo,
       regexp_replace(regexp_replace(s.source_url,'^https://github.com/[^/]+/[^/]+/blob/[^/]+/',''),'/?SKILL\.md$','')::text,
       s.skill_name,s.id,s.source_url,s.content_sha256,s.content_bytes,s.content_object_key,s.compressed_bytes,s.storage_backend,s.first_seen_at,s.last_seen_at,s.retrieved_at,now()
from skillset.skills s
where s.source_url ~ '^https://github.com/[^/]+/[^/]+/blob/[^/]+/.+/SKILL\.md$'
  and s.content_sha256 is not null
on conflict(source,repo_path) do nothing;

update skillset.repositories
set discovery_status='pending', codeload_status='pending', codeload_attempts=0,codeload_error=null,updated_at=now()
where discovery_status='ok';
