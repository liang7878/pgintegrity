-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgintegrity" to load this file. \quit

CREATE FUNCTION pg_integrity()
RETURNS trigger
AS '$libdir/pgintegrity'
LANGUAGE C;
