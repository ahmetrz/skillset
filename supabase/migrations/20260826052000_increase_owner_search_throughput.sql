create or replace function public.skillset_claim_owner_search_batch(p_limit integer default 5)
returns table(owner text)
language plpgsql
security definer
set search_path to 'skillset','public'
as $function$
begin
 return query
 with picked as (
   select o.owner from skillset.owner_search o
   where o.status in ('pending','retry') or (o.status='processing' and o.updated_at<now()-interval '10 minutes')
   order by o.attempts,o.owner
   for update skip locked
   limit greatest(1,least(p_limit,10))
 ), claimed as (
   update skillset.owner_search o
   set status='processing',attempts=attempts+1,updated_at=now()
   from picked p where o.owner=p.owner
   returning o.owner
 ) select * from claimed;
end;$function$;