CREATE SCHEMA stg_fia;

-- dim
CREATE TABLE stg_fia.team_dim (
	id SERIAL PRIMARY KEY,
	name VARCHAR(50),
	source_key VARCHAR(25)
);
