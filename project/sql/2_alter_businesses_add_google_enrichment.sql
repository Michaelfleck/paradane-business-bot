-- Migration patch: Add missing column 'google_enrichment' to 'businesses' table

alter table public.businesses
add column if not exists google_enrichment jsonb;