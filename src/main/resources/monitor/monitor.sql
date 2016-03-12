CREATE TABLE IF NOT EXISTS monitoring (
	lastRan bigint,
	duration integer,
	island varchar(30),
	query varchar(3000)
);

CREATE TABLE IF NOT EXISTS migrationstats (
  migrationstatsID serial PRIMARY KEY,
  fromLoc varchar(250),
  toLoc varchar(250),
  objectFrom varchar(250),
  objectTo varchar(250),
  startTime bigint,
  endTime bigint,
  countExtracted bigint,
  countLoaded bigint,
  message varchar(1000)
);
