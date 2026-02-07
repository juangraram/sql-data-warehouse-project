/*
===============================================================================
BULK Script: To insert data
===============================================================================
Script Purpose:
    This script import a large volume of data from external data file
    like CSV or TEXT file, into a database table, using a Store Procedure. 
    Please be sure.
      To pass the exactly PATH where is located your file.
	  Run this script to import a large volume of data.
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Track the ETL time, that takes upload a table
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;
    BEGIN TRY

        SET @start_batch_time = GETDATE();

        -- This is called a Full load, to guarantee the full load, be sure you Truncate the table before BULK insert
        PRINT '================================================================';
        PRINT 'Loading Bronze Layer'
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------------------------------';


        


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '\\wsl.localhost\Ubuntu-24.04\home\juangraram\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        -- Use the cluase "WITH" to know how SQL will handle our CSV file
        WITH (
            -- With the below clause we tell SQL the first row will be the header of the file.
            FIRSTROW = 2,
            -- With the below clause we tell SQL what is the separetor or delimiter. ', ; # " '.
            FIELDTERMINATOR = ',',
            -- The below cluase block the entire table, while the process of upload data is working.
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        -- Test the quality of the data file.
        -- Check that the data has no shifted and is in the correct columns.
        -- Check that the data has the same headers name.
        -- Check that the data has the same row numbers.

        -- SELECT * FROM bronze.crm_cust_info;

        -- SELECT COUNT(*) FROM bronze.crm_cust_info;

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '\\wsl.localhost\Ubuntu-24.04\home\juangraram\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        -- Use the cluase "WITH" to know how SQL will handle our CSV file
        WITH (
            -- With the below clause we tell SQL the first row will be the header of the file.
            FIRSTROW = 2,
            -- With the below clause we tell SQL what is the separetor or delimiter. ', ; # " '.
            FIELDTERMINATOR = ',',
            -- The below cluase block the entire table, while the process of upload data is working.
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        -- SELECT * FROM bronze.crm_prd_info;

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '\\wsl.localhost\Ubuntu-24.04\home\juangraram\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        -- Use the cluase "WITH" to know how SQL will handle our CSV file
        WITH (
            -- With the below clause we tell SQL the first row will be the header of the file.
            FIRSTROW = 2,
            -- With the below clause we tell SQL what is the separetor or delimiter. ', ; # " '.
            FIELDTERMINATOR = ',',
            -- The below cluase block the entire table, while the process of upload data is working.
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        -- SELECT * FROM bronze.crm_sales_details;

        PRINT '================================================================';
        PRINT '----------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '\\wsl.localhost\Ubuntu-24.04\home\juangraram\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        -- Use the cluase "WITH" to know how SQL will handle our CSV file
        WITH (
            -- With the below clause we tell SQL the first row will be the header of the file.
            FIRSTROW = 2,
            -- With the below clause we tell SQL what is the separetor or delimiter. ', ; # " '.
            FIELDTERMINATOR = ',',
            -- The below cluase block the entire table, while the process of upload data is working.
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        -- SELECT * FROM bronze.erp_cust_az12;

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '\\wsl.localhost\Ubuntu-24.04\home\juangraram\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        -- Use the cluase "WITH" to know how SQL will handle our CSV file
        WITH (
            -- With the below clause we tell SQL the first row will be the header of the file.
            FIRSTROW = 2,
            -- With the below clause we tell SQL what is the separetor or delimiter. ', ; # " '.
            FIELDTERMINATOR = ',',
            -- The below cluase block the entire table, while the process of upload data is working.
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        -- SELECT * FROM bronze.erp_loc_a101;

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '\\wsl.localhost\Ubuntu-24.04\home\juangraram\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        -- Use the cluase "WITH" to know how SQL will handle our CSV file
        WITH (
            -- With the below clause we tell SQL the first row will be the header of the file.
            FIRSTROW = 2,
            -- With the below clause we tell SQL what is the separetor or delimiter. ', ; # " '.
            FIELDTERMINATOR = ',',
            -- The below cluase block the entire table, while the process of upload data is working.
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        PRINT '============================================================================================';

        PRINT '>> Total duration of load the entire bronze layer';

        SET @end_batch_time = GETDATE();
        PRINT '>> Load Duration bronze layer: ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' seconds';

        -- SELECT * FROM bronze.erp_px_cat_g1v2;
    END TRY
    BEGIN CATCH
        PRINT '============================================================================';
        PRINT 'Error occured during loading Bronze Layer';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '============================================================================';
    END CATCH
END