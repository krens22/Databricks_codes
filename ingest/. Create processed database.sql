--Create managed tables, so we don't specify the location, we want the location to be picked up from the database itself
-- Any table you create(managed table) would be created under this mount, if location not specified the table will be created under the the databricks default location
CREATE DATABASE IF NOT EXISTS f1_proc
LOCATION "/mnt/form1ka22/proc"

-- Checking the raw database storage
DESC DATABASE f1_raw

-- Checking the proc database storage
DESC DATABASE f1_proc
