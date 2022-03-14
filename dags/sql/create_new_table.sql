CREATE TABLE IF NOT EXISTS {{params.table_name}} (
    city VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    temp NUMERIC(6, 4) NOT NULL,
    feels_like_temp NUMERIC(6, 4) NOT NULL,
    min_temp NUMERIC(6, 4) NOT NULL,
    max_temp NUMERIC(6, 4) NOT NULL,
    humidity NUMERIC(6, 4) NOT NULL,
    clouds NUMERIC(6, 4) NOT NULL
);