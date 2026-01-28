/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


-- Create a database called "DataWarehouse"

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database, if exists.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create "DataWarehouse" database
CREATE DATABASE DataWarehouse;
GO

-- Use the new database created
USE DataWarehouse;
GO

-- Create schemas to help you mantain the things organize (Like a Folder)
-- "GO" it is a batch separator Think of it as a "signal" to the tool to wrap up everything written so far, send it to the server as a single unit (a batch), and wait for it to finish before moving on to the next section.
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
