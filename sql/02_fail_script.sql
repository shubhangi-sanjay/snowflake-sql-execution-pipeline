-- Intentionally failing script for dependency testing

ALTER TABLE GIT_SCHEMA.EMPLOYEE
ADD COLUMN ADDRESS VARCHAR(255);

-- Run this script twice OR
-- Run after a column already exists → guaranteed failure
