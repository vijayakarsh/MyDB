DROP TABLE IF EXISTS LOG_DefragementationStatus; 
CREATE OR REPLACE TABLE LOG_DefragementationStatus
(	
	Id 				INT UNSIGNED AUTO_INCREMENT,
	tableName 		VARCHAR(100),
	defragBefore	FLOAT,
	defragAfter		FLOAT,
	activeFlag		BIT, -- 1: currently active defrag job , 0 : historical defrag job
	statusFlag  	BIT, -- 1: Defrag in process, 0: completed
	createDate  	DATETIME,
	editDate  		DATETIME,
	PRIMARY KEY (Id)
) ENGINE = InnoDB;

DELIMITER //
CREATE OR REPLACE DEFINER=CURRENT_USER PROCEDURE usp_ShowDefragmentStatus()
SQL SECURITY DEFINER 
BEGIN 
	
	SELECT tmp.tableName,
			CASE WHEN  lds.tableName IS NULL 
						THEN CONCAT('Defragmentation not started for ' , tmp.tableName)
				  WHEN lds.statusFlag = 1 AND lds.createDate IS NOT NULL AND lds.editDate IS NULL
				  		THEN CONCAT('Defragmenting in progress, defragmenting ', lds.tableName, ' STARTED AT ', CAST(lds.createDate as CHAR(100)), ' Percent complete is the next result set') 
			     WHEN lds.statusFlag = 0 AND lds.editDate IS NOT NULL 
				  		THEN CONCAT('Defragmentation completed ', lds.tableName,'. Time taken: ', CAST(TIMEDIFF(lds.editDate, lds.createDate) AS CHAR(100))) 				 
				  ELSE 'NA' END defragmentationStatus, 
			CASE WHEN lds.statusFlag IN (0,1)
						THEN lds.defragBefore 
				  ELSE tmp.defragRatio  END AS fragmentation_Before_DeFrag,
			CASE WHEN lds.statusFlag = 1 
						THEN NULL 
				  WHEN lds.statusFlag = 0 
				  		THEN lds.defragAfter
				  ELSE NULL END AS fragmentation_After_DeFrag,
			CASE WHEN lds.statusFlag = 0 
						THEN 'Completed'
				  WHEN lds.statusFlag = 1
				  		THEN 'In Progress'
				  ELSE '' End status,	  			  
			lds.createDate  AS startDate,
			lds.editDate  	 AS endDate
	FROM 	TBL_Statistics ts INNER JOIN
	(
		SELECT TABLE_NAME tableName,
				 (data_free/(index_length+data_length)) * 100 defragRatio
		FROM 	 information_schema.TABLES
		WHERE  TABLE_SCHEMA =  SCHEMA()
	) tmp ON 
		ts.table_name = tmp.tableName
	LEFT JOIN LOG_DefragementationStatus lds ON 
	 	 tmp.tableName =  lds.tableName
	AND lds.activeflag <> 0; 
	
	IF EXISTS (SELECT 1 FROM LOG_DefragementationStatus WHERE statusFlag = 1 And  activeFlag = 1) THEN 
		SHOW STATUS LIKE '%Innodb_onlineddl_pct_progress%';
	END IF;	
END;

CREATE OR REPLACE DEFINER=CURRENT_USER PROCEDURE usp_TablesOptimization(frag_ratio_limit INT, table_row_limit INT)
SQL SECURITY INVOKER
BEGIN 
	
		DECLARE loopVar			 INT DEFAULT 1;
	
/*######################################################################################

			START SECTION TO SET VALUES FOR VARIABLES TO PROCEED ON DE-FRAGMENTATION

######################################################################################*/

	-- GET STATUS OF GLOBAL VARIABLES
	SELECT CASE WHEN VARIABLE_NAME = 'old_alter_table' THEN IF(SESSION_VALUE = 'ON', 1, 0) ELSE 0 END,
		   CASE WHEN VARIABLE_NAME = 'old_alter_table' THEN IF(SESSION_VALUE = 'ON', 1, 0) ELSE 0 END
		   INTO @old_alter_table, @file_per_table 
	FROM information_schema.SYSTEM_VARIABLES;	   	

	-- RESET FLAG IN LOG TABLE TO DISPLAY STATUS FOR CURRENT RUN
	--	UPDATE LOG_DefragementationStatus SET  activeFlag = 0 WHERE activeFlag = 1;
		TRUNCATE TABLE LOG_DefragementationStatus; 
		TRUNCATE TABLE TBL_Statistics;
	
/*######################################################################################

			END SECTION TO SET VALUES FOR VARIABLES TO PROCEED ON DE-FRAGMENTATION

######################################################################################*/	
	
	-- CHECK IF SYSTEM_VARIBALES ARE SET RIGHT TO DO IN-PLACE REBUILD
	IF @old_alter_table = 0 AND @file_per_table = 1 THEN		

		CREATE OR REPLACE TABLE TBL_Statistics(Id INT AUTO_INCREMENT, table_name VARCHAR(100),  row_count INT, frag_ratio FLOAT , PRIMARY KEY (Id));
		
		-- GET TABLES TO OPTIMIZE
		INSERT TBL_Statistics (table_name, row_count, frag_ratio)
		SELECT TABLE_NAME, TABLE_ROWS, frag_ratio
		FROM 
		(
			Select TABLE_NAME, 
					 TABLE_ROWS,
					 (data_free/(index_length+data_length)) * 100 frag_ratio
			FROM 	 information_schema.TABLES
			WHERE  TABLE_SCHEMA =  SCHEMA()
		) DRV 
		WHERE TABLE_ROWS > 0 AND TABLE_ROWS <  table_row_limit
		AND   frag_ratio >  frag_ratio_limit
		ORDER BY 3 DESC;			

		-- LOOP TO BUILD THE OPTIMIZE STATEMENT
		WHILE (SELECT COUNT(1) FROM TBL_Statistics) >= loopVar DO 
			SELECT  row_count,
					  frag_ratio,
					  table_name
			INTO @tbl_rows, @frag_ratio, @tbl_name		  
			FROM TBL_Statistics 
			WHERE Id = 	loopVar; 
		  				  		
			IF @frag_ratio > frag_ratio_limit AND @tbl_rows < table_row_limit THEN 
				SET @sqltext = CONCAT('ALTER TABLE ', @tbl_name, ' ENGINE = INNODB, LOCK = NONE, ALGORITHM = INPLACE;');
				PREPARE _sqlText FROM @sqltext;

				-- LOG STATUS BEFORE REBUILD
					INSERT LOG_DefragementationStatus (tableName, statusFlag, defragBefore, activeFlag, createDate) VALUES (@tbl_name, 1, @frag_ratio, 1, now()); 
						SET @lid = LAST_INSERT_ID(); 
							  	EXECUTE _sqlText;
							  -- SLEEP(2);
							  SELECT (data_free/(index_length+data_length)) * 100	INTO @frag_ratio_after FROM information_schema.TABLES	 WHERE TABLE_SCHEMA =  SCHEMA() AND TABLE_NAME = @tbl_name;
							  
				-- UPDATE STATUS AFTER REBUILD	
					UPDATE LOG_DefragementationStatus SET statusFlag = 0, defragAfter = @frag_ratio_after, editDate = now()
					WHERE Id = @lid;					
			END IF; 
	
			SET loopVar = loopVar + 1; 
		END WHILE;	
			
	END IF;		
END;  
// 
DELIMITER ;
