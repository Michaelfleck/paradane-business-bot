-- SQL migration for creating 'businesses' table in Supabase
-- Run this in Supabase SQL editor or via CLI migration

create table if not exists public.businesses (
    id text primary key,
    alias text,
    name text,
    image_url text,
    url text,
    phone text,
    display_phone text,
    price text,
    state text,
    country text,
    zip_code text,
    city text,
    address1 text,
    address2 text,
    address3 text,
    cross_streets text,
    date_opened text,
    date_closed text,
    yelp_menu_url text,
    cbsa text,
    primary_category text,
    score text,
    distance text,
    business_status text,
    formatted_address text,
    formatted_phone_number text,
    international_phone_number text,
    geometry text,
    opening_hours text,
    user_ratings_total text,
    type text,
    website text,
    rating text

    review_count integer,
    photo_count integer,

    is_closed boolean,
    is_claimed boolean,

    rating float,
    latitude float,
    longitude float,
    response_rate float,

    categories jsonb,
    coordinates jsonb,
    transactions jsonb,
    location jsonb,
    display_address jsonb,
    attributes jsonb,
    photos jsonb,
    special_hours jsonb,
    messaging jsonb,
    photo_details jsonb,
    popularity_score jsonb,
    rapc jsonb,
    hours jsonb,

    created_at timestamp with time zone default timezone('utc'::text, now())
    updated_at timestamp with time zone default timezone('utc'::text, now())
);