-- dim
CREATE TABLE team_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(25),
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE driver_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	code VARCHAR(10),
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
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
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE race_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(100),
	date_id INT,
	circuit_id INT,
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
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
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE qualifying_dim (
	id SERIAL PRIMARY KEY,
	position INT,
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE laps_stats_dim (
	id SERIAL PRIMARY KEY,
	laps_count INT,
	fastest_lap_time_in_milliseconds INT,
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE position_stats_dim (
	id SERIAL PRIMARY KEY,
	starting_position INT,
	finishing_position INT,
	is_incremental_load BOOLEAN,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE points_stats_dim (
	id SERIAL PRIMARY KEY,
	points INT,
	is_fastest_lap BOOLEAN,
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
	source_key VARCHAR(25)
);

-- dim
CREATE TABLE pit_stops_stats_dim (
	id SERIAL PRIMARY KEY,
	pit_stops_count INT,
	summary_pit_stops_time_in_milliseconds INT,
	valid_from TIMESTAMP,
	valid_to TIMESTAMP,
	is_incremental_load BOOLEAN,
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
	laps_stats_id INT,
	position_stats_id INT,
	points_stats_id INT,
	pit_stops_stats_id INT,
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
			REFERENCES qualifying_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_laps_stats_id
		FOREIGN KEY (laps_stats_id)
			REFERENCES laps_stats_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_position_stats_id
		FOREIGN KEY (position_stats_id)
			REFERENCES position_stats_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_points_stats_id
		FOREIGN KEY (points_stats_id)
			REFERENCES points_stats_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_pit_stops_stats_id
		FOREIGN KEY (pit_stops_stats_id)
			REFERENCES pit_stops_stats_dim (id) ON DELETE SET NULL
);