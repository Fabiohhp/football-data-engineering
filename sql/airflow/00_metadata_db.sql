-- Airflow's metadata database.
--
-- CLAUDE.md §2 puts the Gold warehouse and Airflow's metadata in the same
-- PostgreSQL instance, but they get separate databases and separate roles: the
-- warehouse is the project's deliverable and its contents are the contract with
-- the downstream subprojects, while Airflow's metadata is scheduler bookkeeping
-- that churns constantly. Nothing that reads Gold should be able to see
-- task_instance, and an Airflow migration should never be able to touch a fact
-- table.
--
-- This is NOT in sql/ddl/, which Postgres runs from docker-entrypoint-initdb.d
-- on first startup only. It has to be re-runnable against an already-populated
-- volume, because anyone who ran this stack before Airflow existed already has
-- an initialized data directory that initdb.d will never revisit. Hence the
-- \gexec guards below, which make the whole file idempotent.
--
-- Run by the `airflow-metadb` service in docker-compose.yml; the role name,
-- password and database name arrive as psql variables from .env.
--
-- CREATE ROLE / CREATE DATABASE cannot run inside a transaction block, so this
-- file must be executed in psql's default autocommit mode (no -1 / --single-
-- transaction). ON_ERROR_STOP is still set by the caller.

\set ON_ERROR_STOP on

-- format() with %I/%L quotes the identifier and the password literal properly,
-- so a password containing a quote cannot break out into injected SQL.
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'db_user', :'db_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'db_user')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', :'db_name', :'db_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db_name')
\gexec

-- Keep the password in step with .env on re-runs: without this, changing
-- AIRFLOW_DB_PASSWORD would leave the role holding the old one and Airflow
-- would fail to authenticate with no obvious cause.
SELECT format('ALTER ROLE %I WITH PASSWORD %L', :'db_user', :'db_password')
\gexec
