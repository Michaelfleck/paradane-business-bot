-- Migration patch: Add zoho_lead_id column to businesses table

alter table public.businesses
add column if not exists zoho_lead_id text;