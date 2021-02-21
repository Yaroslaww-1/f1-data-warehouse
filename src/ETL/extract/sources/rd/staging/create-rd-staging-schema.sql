CREATE SCHEMA stg_rd;

CREATE TABLE stg_rd.circuit_dim (
	id SERIAL PRIMARY KEY,
	ref VARCHAR(25),
	name VARCHAR(100),
	source_key VARCHAR(25) UNIQUE
);

-- dim
CREATE TABLE stg_rd.race_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(100),
	date DATE,
	circuit_id INT,
	source_key VARCHAR(25) UNIQUE,
	CONSTRAINT fk_circuit_id
		FOREIGN KEY (circuit_id)
			REFERENCES stg_wc.circuit_dim (id) ON DELETE SET NULL
);