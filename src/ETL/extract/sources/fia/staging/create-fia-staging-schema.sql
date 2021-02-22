CREATE SCHEMA stg_fia;

CREATE TABLE stg_fia.qualifying_dim (
	id SERIAL PRIMARY KEY,
	year INT,
	position INT,
	venue VARCHAR(25),
	driver_name VARCHAR(25),
	driver_code VARCHAR(10),
	source_key VARCHAR(25)
);
