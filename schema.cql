CREATE KEYSPACE IF NOT EXISTS glc_disaster_ks
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE glc_disaster_ks;

CREATE TABLE IF NOT EXISTS landslide_events (
    country_code text,
    event_year int,
    event_date timestamp,
    event_id bigint,
    source_name text,
    source_link text,
    event_time text,
    event_title text,
    event_description text,
    location_description text,
    location_accuracy text,
    landslide_category text,
    landslide_trigger text,
    landslide_size text,
    landslide_setting text,
    fatality_count int,
    injury_count int,
    storm_name text,
    photo_link text,
    notes text,
    event_import_source text,
    event_import_id double,
    country_name text,
    admin_division_name text,
    admin_division_population bigint,
    gazeteer_closest_point text,
    gazeteer_distance double,
    submitted_date timestamp,
    created_date timestamp,
    last_edited_date timestamp,
    longitude double,
    latitude double,
    PRIMARY KEY ((country_code, event_year), event_date, event_id)
) WITH CLUSTERING ORDER BY (event_date DESC, event_id ASC);