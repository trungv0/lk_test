create table trung.event (
    id varchar primary key,
    created_at timestamptz not null,
    ip_address varchar not null,
    device_id varchar not null,
    user_id varchar,
    uuid varchar not null,
    event_type varchar not null,
    device_type varchar,  -- app
    platform varchar,  -- app
    event_properties varchar(max),  -- app
    user_email varchar,  -- web
    metadata varchar(max)  -- web
);

create table trung.event_sequence (
    id varchar primary key,
    user_id varchar not null,
    previous_id varchar,
    next_id varchar
);

create table trung.event_metrics (
    "date" date primary key,
    nb_app_events integer not null default 0,
    nb_web_events integer not null default 0
);

create table trung.attribution (
    user_id varchar primary key,
    visited_at timestamptz not null,
    utm_campaign varchar,
    utm_content varchar,
    utm_device varchar,
    utm_medium varchar,
    utm_source varchar,
    utm_term varchar
);
