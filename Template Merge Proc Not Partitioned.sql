/****** Object:  StoredProcedure [<schema_name>].[merge_<table_name>_hist]    Script Date: 6/7/2018 10:41:22 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [<schema_name>].[merge_<table_name>_hist] @pipeline_run_id [varchar](150),@parent_pipeline_run_id [varchar](150) AS

/*===========================================================================================================================
Name:        [<schema_name>].[merge_<table_name>_hist]

Type:        Stored Procedure

Description: Loads the staging table [<schema_name>].[<table_name>_hist], which is a permanent staging table
			 that maintains historical data.  It will be a source table for the DWH facts and dimensions.
                
Sources:     <table_name> from <source system>
               
Used By:     Azure SQL DWH

Changes:     <datetime>, <name>, Inital
											                    
===========================================================================================================================*/



BEGIN

	--Declare variables
	DECLARE @error_message varchar(1000);
	DECLARE @table_name varchar(100);
	DECLARE @run_id varchar(50);

	--This ID is saved to the logging tables used in this procedure.
	SET @run_id  = NEWID();

	--The history table should already exist. Throw an error and exit if it does not.
	IF OBJECT_ID('<schema_name>.<table_name>_hist') IS NULL
		BEGIN

			SET @table_name = '<schema_name>.<table_name>_hist'; 
			SET @error_message = 'The table ' +  @table_name + ' does not exist.';

			THROW 50000, @error_message, 1;

		END


	/*
	Drop any work tables if they already exist
	*/
	IF OBJECT_ID('<schema_name>.<table_name>_deletes_work') IS NOT NULL
		BEGIN
			DROP TABLE [<schema_name>].[<table_name>_deletes_work]
		END


	IF OBJECT_ID('<schema_name>.<table_name>_incr_work') IS NOT NULL
		BEGIN
			DROP TABLE [<schema_name>].[<table_name>_incr_work]
		END

	IF OBJECT_ID('<schema_name>.<table_name>_hist_work') IS NOT NULL
		BEGIN
			DROP TABLE [<schema_name>].[<table_name>_hist_work]
		END
			
	IF OBJECT_ID('<schema_name>.<table_name>_qa_counts_work') IS NOT NULL
		BEGIN
			DROP TABLE [<schema_name>].[<table_name>_qa_counts_work]
		END


	/*
	The table [<schema_name>].[<table_name>_incr] is a landing table that is an internal copy of the external (blob) data. Add
	the key_sequence_num so that we can remove duplicates in the next step. For each key we only want the most recent row from the source system.
	If there were multiple external files we could have a key more than once.
	*/
	CREATE TABLE  [<schema_name>].[<table_name>_incr_work]
	WITH
	(   DISTRIBUTION = <distribution>, <index>
	)
	AS

	SELECT	*
			,ROW_NUMBER() OVER(PARTITION BY <primary_key>, sys_environment_name order by sys_file_dt desc) as  key_sequence_num
	FROM	[<schema_name>].[<table_name>_incr] 
	OPTION	(LABEL  = 'INSERT : [<schema_name>].[<table_name>_incr_work]')


	
	--If the count below is zero that means there was no data to import from the BLOB storage. Exit the procedure now.
	IF (SELECT COUNT(*) FROM [<schema_name>].[<table_name>_incr_work]) <> 0 

	BEGIN


		/*
		Find and save any deletes to a work table
		*/
		CREATE TABLE  [<schema_name>].[<table_name>_deletes_work]
		WITH
		(   DISTRIBUTION = <distribution>, <index>
		)
		AS


		SELECT		[CacheDel_Table_Key] as <primary_key>, cache_deletes_hist.sys_environment_name
		FROM		[<schema_name>].[cache_deletes_hist]
		INNER JOIN	[<schema_name>].[si_table_ids_gvw_hist]
		ON			cache_deletes_hist.[CacheDel_TblID_Key] = si_table_ids_gvw_hist.[tblID_Key]
		AND			cache_deletes_hist.[sys_environment_name] = si_table_ids_gvw_hist.[sys_environment_name]
		WHERE		si_table_ids_gvw_hist.tblId_name = '<table_name>' 

		UNION ALL

		SELECT		[FPSynch_Record_Key] as <primary_key>, fp_synch_log_hist.sys_environment_name
		FROM		[<schema_name>].[fp_synch_log_hist]
		WHERE		[FPSynch_Table_Name] = '<table_name>'
		AND			FPSynch_type = 0
		OPTION		(LABEL  = 'INSERT : [<schema_name>].[<table_name>_deletes_work]')



		/*
		Set variables for the main CTAS merge query.
		*/
		DECLARE @current_datetime datetime2
		DECLARE @end_datetime datetime2
		
		SET	@current_datetime = GETDATE()
		SET	@end_datetime = (GETDATE() - 0.00000005)


		CREATE TABLE  [<schema_name>].[<table_name>_hist_work]
		WITH
		(   DISTRIBUTION = <distribution>, <index>
		)
		AS



		/*
		Find the unchanged rows. The ts column tells us whether the row has been changed.
		*/
		SELECT		<columns>, 
					 sys_file_name, 
					 sys_file_dt, 
					 sys_environment_name, 
					 sys_source_system_name, 
					 sys_table_name, 
					 sys_extract_job_name,  
					 [sys_effective_from_dt],
					 [sys_effective_to_dt],
					 [sys_current_ind],
					 [sys_source_deleted_ind],
					 [sys_inserted_dt],
					 [sys_inserted_by],
					 [sys_updated_dt],
					 [sys_updated_by]  
		FROM  [<schema_name>].[<table_name>_hist] as hist  
		WHERE [sys_current_ind] = 1
		AND NOT  EXISTS (SELECT 1
							FROM   [<schema_name>].[<table_name>_incr_work] as incr
							WHERE incr.<primary_key> = hist.<primary_key> 
							AND incr.sys_environment_name = hist.sys_environment_name
							AND incr.ts <> hist.ts 
							AND incr.sys_file_dt > hist.sys_file_dt)
		AND NOT EXISTS (SELECT 1
						FROM [<schema_name>].[<table_name>_deletes_work] del
						WHERE del.<primary_key> = hist.<primary_key> 
						AND del.sys_environment_name = hist.sys_environment_name)
				


		UNION ALL

		/*
		Find the unchanged rows.
		*/
		SELECT		<columns>, 
					 sys_file_name, 
					 sys_file_dt, 
					 sys_environment_name, 
					 sys_source_system_name, 
					 sys_table_name, 
					 sys_extract_job_name,  
					 [sys_effective_from_dt],
					 [sys_effective_to_dt],
					 [sys_current_ind],
					 [sys_source_deleted_ind],
					 [sys_inserted_dt],
					 [sys_inserted_by],
					 [sys_updated_dt],
					 [sys_updated_by]  
		FROM		 [<schema_name>].[<table_name>_hist]
		WHERE		 [sys_current_ind] = 0



		UNION ALL


		/*
		Find the new rows. The new rows here represent both inserts and updates from the source system. Each row being added must be newer than 
		what is currently in the staging table. This will always be the case unless an old blob file is being reprocessed. Also, if there are multiple 
		changes for a single key then take only the most recent.
		*/
		SELECT		<columns>, 
					 sys_file_name, 
					 sys_file_dt, 
					 sys_environment_name, 
					 sys_source_system_name, 
					 sys_table_name, 
					 sys_extract_job_name,  
					 @current_datetime as [sys_effective_from_dt],
					 convert(datetime2, '9999-12-31')  as [sys_effective_to_dt],
					 1 as [sys_current_ind],
					 0 as [sys_source_deleted_ind],
					 GETDATE() as [sys_inserted_dt],
					 [sys_extract_run_id] as [sys_inserted_by],
					 @current_datetime as [sys_updated_dt],
					 convert(varchar(150), 'CTAS Insert') as [sys_updated_by]	
		FROM		 [<schema_name>].[<table_name>_incr_work] as incr
		WHERE NOT EXISTS (SELECT 1
						 FROM  [<schema_name>].[<table_name>_hist] as hist
						 WHERE incr.<primary_key> = hist.<primary_key>
						 AND incr.sys_environment_name = hist.sys_environment_name
						 AND incr.ts = hist.ts)
		AND NOT EXISTS (SELECT 1
						FROM  [<schema_name>].[<table_name>_hist] as hist
						WHERE incr.<primary_key> = hist.<primary_key>
						AND incr.sys_environment_name = hist.sys_environment_name
						AND incr.sys_file_dt <= hist.sys_file_dt)  --Incoming data file must be newer than past files.
		AND  NOT EXISTS (SELECT 1
						 FROM [<schema_name>].[<table_name>_deletes_work] del
						 WHERE incr.<primary_key> = del.<primary_key>
						 AND del.sys_environment_name = incr.sys_environment_name)
		AND	key_sequence_num = 1  --Only the most recent row



		UNION ALL


		/*
		Expired rows from any source system updates
		*/
		SELECT		<columns>, 
					 sys_file_name, 
					 sys_file_dt, 
					 sys_environment_name, 
					 sys_source_system_name, 
					 sys_table_name, 
					 sys_extract_job_name,  
					 [sys_effective_from_dt],
					 @end_datetime  as [sys_effective_to_dt],
					 0 as [sys_current_ind],
					 0 as [sys_source_deleted_ind],
					 [sys_inserted_dt],
					 [sys_inserted_by],
					 @current_datetime as [sys_updated_dt],
					 convert(varchar(150), 'CTAS Update') as [sys_updated_by]
		FROM		 [<schema_name>].[<table_name>_hist] as hist  
		WHERE		 [sys_current_ind] = 1
		AND  EXISTS (SELECT 1	
					 FROM [<schema_name>].[<table_name>_incr_work] as incr
					 WHERE incr.<primary_key> = hist.<primary_key>
					 AND incr.sys_environment_name = hist.sys_environment_name
					 AND incr.ts <> hist.ts
					 AND incr.sys_file_dt > hist.sys_file_dt)
		AND  NOT EXISTS (SELECT 1
						 FROM [<schema_name>].[<table_name>_deletes_work] del
						 WHERE del.<primary_key> = hist.<primary_key>
						 AND del.sys_environment_name = hist.sys_environment_name)



		UNION ALL

		/*
		Expired rows from any source system deletes
		*/
		SELECT		<columns>, 
					 sys_file_name, 
					 sys_file_dt, 
					 sys_environment_name, 
					 sys_source_system_name, 
					 sys_table_name, 
					 sys_extract_job_name,  
					 [sys_effective_from_dt],
					 @end_datetime as [sys_effective_to_dt],
					 0 as [sys_current_ind],
					 1 as [sys_source_deleted_ind],
					 [sys_inserted_dt],
					 [sys_inserted_by],
					 @current_datetime as [sys_updated_dt],
					 convert(varchar(150), 'CTAS Delete') as [sys_updated_by]
		FROM		 [<schema_name>].[<table_name>_hist] as hist  
		WHERE		 [sys_current_ind] = 1
		AND  EXISTS (SELECT 1
					 FROM [<schema_name>].[<table_name>_deletes_work] del
					 WHERE del.<primary_key> = hist.<primary_key>
					 AND del.sys_environment_name = hist.sys_environment_name)
		OPTION		(LABEL  = 'CTAS : [<schema_name>].[<table_name>_hist_work]')


		
		/*
		Log the counts for QA
		*/
	
		CREATE TABLE [<schema_name>].[<table_name>_qa_counts_work]
		WITH
		(   DISTRIBUTION = HASH(sys_file_name)
		)
		AS


		SELECT		incr.<primary_key>,
					incr.sys_file_name,
					incr.sys_environment_name,
					incr.sys_table_name,
					incr.sys_source_system_name,
					'CTAS New Row'  as count_desc 
		FROM		[<schema_name>].[<table_name>_incr_work] incr
		WHERE		incr.key_sequence_num = 1
		AND			EXISTS (SELECT 1
							FROM [<schema_name>].[<table_name>_hist_work] as hist 	
							WHERE hist.<primary_key> = incr.<primary_key>
							AND	incr.sys_environment_name = hist.sys_environment_name
							AND hist.sys_updated_dt =  @current_datetime
							AND hist.sys_current_ind = 1)




		UNION ALL

		
		SELECT		incr.<primary_key>,
					incr.sys_file_name,
					incr.sys_environment_name,
					incr.sys_table_name,
					incr.sys_source_system_name,
					'CTAS No Change'  as count_desc
		FROM		[<schema_name>].[<table_name>_incr_work] incr
		WHERE		incr.key_sequence_num = 1
		AND			EXISTS (SELECT 1
							FROM [<schema_name>].[<table_name>_hist_work] as hist 	
							WHERE hist.<primary_key> = incr.<primary_key>
							AND	incr.sys_environment_name = hist.sys_environment_name
							AND hist.sys_updated_dt <>  @current_datetime
							AND hist.sys_current_ind = 1)



		UNION ALL


		SELECT		incr.<primary_key>,
					incr.sys_file_name,
					incr.sys_environment_name,
					incr.sys_table_name,
					incr.sys_source_system_name,
					'Duplicate In External Files'  as count_desc
		FROM		[<schema_name>].[<table_name>_incr_work] incr
		WHERE		incr.key_sequence_num > 1


		UNION ALL

	
		
		SELECT		incr.<primary_key>,
					incr.sys_file_name,
					incr.sys_environment_name,
					incr.sys_table_name,
					incr.sys_source_system_name,
					'Deleted From Source'  as count_desc  
		FROM		[<schema_name>].[<table_name>_incr_work] incr
		WHERE		incr.key_sequence_num = 1
		AND EXISTS (SELECT 1
					FROM [<schema_name>].[<table_name>_deletes_work] del
					WHERE 	del.<primary_key> = incr.<primary_key>
					AND			incr.sys_environment_name = del.sys_environment_name)
		OPTION  (LABEL  = 'CTAS :  [<schema_name>].[<table_name>_qa_counts_work')



		INSERT INTO [etl_config].[stage_load_counts_log] 
					(batch_id,
					dest_file_name,
					environment_name,
					table_name,
					source_system_name,
					count_desc,
					total_rows,
					sys_inserted_dt,
					sys_inserted_by,
					parent_pipeline_run_id)
		SELECT		@run_id as batch_id,
					sys_file_name,
					sys_environment_name,
					sys_table_name,
					sys_source_system_name,
					count_desc,
					count(*) as total_rows,
					@current_datetime,
					@pipeline_run_id,
					@parent_pipeline_run_id	
		FROM		[<schema_name>].[<table_name>_qa_counts_work]
		GROUP by	sys_file_name,
					sys_environment_name,
					sys_table_name,
					sys_source_system_name,
					count_desc
		OPTION  (LABEL  = 'INSERT : [etl_config].[stage_load_counts_log]')



		/*
		Error Checking and table cleanup.
		*/
		IF EXISTS (SELECT <primary_key>, sys_environment_name
					FROM  [<schema_name>].[<table_name>_hist_work]
					WHERE sys_current_ind = 1
					GROUP BY <primary_key>, sys_environment_name
					HAVING COUNT(*) > 1)
			BEGIN
					SET @table_name = '[<schema_name>].[<table_name>_hist_work]' 

					SET @error_message = 'Duplicate keys found in the table: ' + @table_name;
					THROW 50000, @error_message, 1;
			END

		IF EXISTS (SELECT sys_file_name, count(*) as total_rows			
					FROM [<schema_name>].[<table_name>_incr_work]
					GROUP by sys_file_name

					EXCEPT 

					SELECT dest_file_name, sum(total_rows) as total_rows  
					FROM  [etl_config].[stage_load_counts_log] 
					WHERE batch_id = @run_id
					GROUP BY dest_file_name)

			BEGIN
					SET @error_message = 'External table counts do not match what was processed.';
					THROW 50000, @error_message, 1;
			END

		ELSE 
			BEGIN
					RENAME OBJECT  [<schema_name>].[<table_name>_hist]  TO [<table_name>_hist_previous];

					RENAME OBJECT [<schema_name>].[<table_name>_hist_work] TO [<table_name>_hist];

					DROP TABLE [<schema_name>].[<table_name>_incr_work];

					DROP TABLE  [<schema_name>].[<table_name>_deletes_work];

					DROP TABLE [<schema_name>].[<table_name>_qa_counts_work];

					DROP TABLE [<schema_name>].[<table_name>_hist_previous];

					TRUNCATE TABLE [<schema_name>].[<table_name>_incr];
			END

	END

END 
GO


