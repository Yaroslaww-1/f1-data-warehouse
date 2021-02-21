CREATE SCHEMA stg_wc;

-- dim
CREATE TABLE stg_wc.team_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(25),
	source_key VARCHAR(25) UNIQUE
);

-- dim
CREATE TABLE stg_wc.driver_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	code VARCHAR(10),
	source_key VARCHAR(25) UNIQUE
);

CREATE TABLE stg_wc.circuit_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(100),
	source_key VARCHAR(25) UNIQUE
);

-- dim
CREATE TABLE stg_wc.race_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(100),
	date DATE,
	circuit_id INT,
	source_key VARCHAR(25) UNIQUE,
	CONSTRAINT fk_circuit_id
		FOREIGN KEY (circuit_id)
			REFERENCES stg_wc.circuit_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE stg_wc.status_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(25),
	source_key VARCHAR(25) UNIQUE
);

-- dim
CREATE TABLE stg_wc.qualifying_dim (
	id SERIAL PRIMARY KEY,
	position INT,
	driver_id INT,
	race_id INT,
	team_id INT,
	source_key VARCHAR(25) UNIQUE,
	CONSTRAINT fk_driver_id
		FOREIGN KEY (driver_id)
			REFERENCES stg_wc.driver_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_race_id
		FOREIGN KEY (race_id)
			REFERENCES stg_wc.race_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_team_id
		FOREIGN KEY (team_id)
			REFERENCES stg_wc.team_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE stg_wc.laps_stats_dim (
	id SERIAL PRIMARY KEY,
	driver_id INT,
	race_id INT,
	lap INT,
	time_in_milliseconds INT,
	source_key VARCHAR(25) UNIQUE,
	CONSTRAINT fk_driver_id
		FOREIGN KEY (driver_id)
			REFERENCES stg_wc.driver_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_race_id
		FOREIGN KEY (race_id)
			REFERENCES stg_wc.race_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE stg_wc.pit_stops_stats_dim (
	id SERIAL PRIMARY KEY,
	duration_in_milliseconds INT,
	driver_id INT,
	race_id INT,
	source_key VARCHAR(25) UNIQUE,
	CONSTRAINT fk_driver_id
		FOREIGN KEY (driver_id)
			REFERENCES stg_wc.driver_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_race_id
		FOREIGN KEY (race_id)
			REFERENCES stg_wc.race_dim (id) ON DELETE SET NULL
);

-- fact
CREATE TABLE stg_wc.driver_race_result (
	id SERIAL PRIMARY KEY,
	driver_id INT,
	race_id INT,
	team_id INT,
	starting_position INT,
	finishing_position INT,
	points DECIMAL(15, 1),
	status_id INT,
	source_key VARCHAR(25) UNIQUE,
	CONSTRAINT fk_driver_id
		FOREIGN KEY (driver_id)
			REFERENCES stg_wc.driver_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_race_id
		FOREIGN KEY (race_id)
			REFERENCES stg_wc.race_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_team_id
		FOREIGN KEY (team_id)
			REFERENCES stg_wc.team_dim (id) ON DELETE SET NULL,
	CONSTRAINT fk_status_id
		FOREIGN KEY (status_id)
			REFERENCES stg_wc.status_dim (id) ON DELETE SET NULL
);