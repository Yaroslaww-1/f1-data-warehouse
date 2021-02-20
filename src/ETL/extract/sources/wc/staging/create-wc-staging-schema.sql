-- dim
CREATE TABLE stg_wc_team_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(25)
);

-- dim
CREATE TABLE stg_wc_driver_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	code VARCHAR(10)
);

CREATE TABLE stg_wc_circuit_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(100)
);

-- dim
CREATE TABLE stg_wc_race_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(100),
	date DATE,
	circuit_id INT,
    CONSTRAINT fk_circuit_id
        FOREIGN KEY (circuit_id)
            REFERENCES stg_wc_circuit_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE stg_wc_status_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(25)
);

-- dim
CREATE TABLE stg_wc_qualifying_dim (
	id SERIAL PRIMARY KEY,
	position INT,
	driver_id INT,
	race_id INT,
	team_id INT,
	CONSTRAINT fk_driver_id
        FOREIGN KEY (driver_id)
            REFERENCES stg_wc_driver_dim (id) ON DELETE SET NULL,
    CONSTRAINT fk_race_id
        FOREIGN KEY (race_id)
            REFERENCES stg_wc_race_dim (id) ON DELETE SET NULL,
    CONSTRAINT fk_team_id
        FOREIGN KEY (team_id)
            REFERENCES stg_wc_team_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE stg_wc_laps_stats_dim (
	id SERIAL PRIMARY KEY,
	driver_id INT,
	race_id INT,
	lap INT,
	time_in_milliseconds INT,
	CONSTRAINT fk_driver_id
        FOREIGN KEY (driver_id)
            REFERENCES stg_wc_driver_dim (id) ON DELETE SET NULL,
    CONSTRAINT fk_race_id
        FOREIGN KEY (race_id)
            REFERENCES stg_wc_race_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE stg_wc_standings_stats_dim (
	id SERIAL PRIMARY KEY,
	driver_id INT,
	race_id INT,
	points INT,
	position INT,
	CONSTRAINT fk_driver_id
        FOREIGN KEY (driver_id)
            REFERENCES stg_wc_driver_dim (id) ON DELETE SET NULL,
    CONSTRAINT fk_race_id
        FOREIGN KEY (race_id)
            REFERENCES stg_wc_race_dim (id) ON DELETE SET NULL
);

-- dim
CREATE TABLE stg_wc_pit_stops_stats_dim (
	id SERIAL PRIMARY KEY,
	duration_in_milliseconds INT,
	driver_id INT,
	race_id INT,
	CONSTRAINT fk_driver_id
        FOREIGN KEY (driver_id)
            REFERENCES stg_wc_driver_dim (id) ON DELETE SET NULL,
    CONSTRAINT fk_race_id
        FOREIGN KEY (race_id)
            REFERENCES stg_wc_race_dim (id) ON DELETE SET NULL
);

-- fact
CREATE TABLE stg_wc_driver_race_result (
	id SERIAL PRIMARY KEY,
	driver_id INT,
	race_id INT,
	team_id INT,
	starting_position INT,
  finishing_position INT,
  points INT,
	CONSTRAINT fk_driver_id
        FOREIGN KEY (driver_id)
            REFERENCES stg_wc_driver_dim (id) ON DELETE SET NULL,
    CONSTRAINT fk_race_id
        FOREIGN KEY (race_id)
            REFERENCES stg_wc_race_dim (id) ON DELETE SET NULL,
    CONSTRAINT fk_team_id
        FOREIGN KEY (team_id)
            REFERENCES stg_wc_team_dim (id) ON DELETE SET NULL
);