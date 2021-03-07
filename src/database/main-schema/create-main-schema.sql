CREATE SCHEMA public;

-- dim
CREATE TABLE team_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(50),
	valid_from TIMESTAMPTZ,
	valid_to TIMESTAMPTZ,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE driver_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	code VARCHAR(10),
	valid_from TIMESTAMPTZ,
	valid_to TIMESTAMPTZ,
	source_key VARCHAR(25)
);

CREATE TYPE month_enum AS ENUM (
	'January',
	'February',
	'March',
	'April',
	'May',
	'June',
	'July',
	'August',
	'September',
	'October',
	'November',
	'December'
);

CREATE TABLE date_dim (
	id SERIAL PRIMARY KEY,
	year INT,
	month month_enum,
	day INT
);

CREATE TABLE circuit_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(100),
	valid_from TIMESTAMPTZ,
	valid_to TIMESTAMPTZ,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE race_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(100),
	date_id INT,
	circuit_id INT,
	valid_from TIMESTAMPTZ,
	valid_to TIMESTAMPTZ,
	source_key VARCHAR(25),
	CONSTRAINT fk_date_id
		FOREIGN KEY (date_id)
			REFERENCES date_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_circuit_id
		FOREIGN KEY (circuit_id)
			REFERENCES circuit_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE status_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(25),
	valid_from TIMESTAMPTZ,
	valid_to TIMESTAMPTZ,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE qualifying_dim (
	id SERIAL PRIMARY KEY,
	position INT,
	valid_from TIMESTAMPTZ,
	valid_to TIMESTAMPTZ,
	source_key VARCHAR(25)
);

-- fact
CREATE TABLE driver_race_result (
	id SERIAL PRIMARY KEY,
	driver_id INT,
	team_id INT,
	race_id INT,
	status_id INT,
	qualifying_id INT,
	laps_count INT,
	fastest_lap_time_in_milliseconds INT,
	starting_position INT,
	finishing_position INT,
	points INT,
	is_fastest_lap BOOLEAN,
	pit_stops_count INT,
	summary_pit_stops_time_in_milliseconds INT,
	CONSTRAINT fk_driver_id
		FOREIGN KEY (driver_id)
			REFERENCES driver_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_team_id
		FOREIGN KEY (team_id)
			REFERENCES team_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_race_id
		FOREIGN KEY (race_id)
			REFERENCES race_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_status_id
		FOREIGN KEY (status_id)
			REFERENCES status_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_qualifying_id
		FOREIGN KEY (qualifying_id)
			REFERENCES qualifying_dim (id) ON DELETE SET NULL
);

CREATE OR REPLACE FUNCTION get_team_name(team_name TEXT)
RETURNS
    TEXT
AS
$BODY$
DECLARE
	team_parts TEXT[];
    team_parts_size INT;
BEGIN
    team_parts = LOWER(string_to_array(team_name, ' ')::TEXT)::TEXT[];
    team_parts_size = array_length(team_parts, 1) - 1;
    IF team_parts_size = 0
    	THEN team_parts_size = 1;
    END IF;
    RETURN array_to_string(team_parts[1:team_parts_size], ' ');
END
$BODY$
LANGUAGE plpgsql;