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

CREATE OR REPLACE DEFINER=CURRENT_USER PROCEDURE usp_Purger(p_startDate      		DATE 		   , 	-- initial start date for date paramester, need not provide time
																	 	  	   p_dateAttribute  	VARCHAR(10)  ,	-- Y, M, Or D attribute to create endDate
																	 	  	   p_attributeValue 	INT  		   ,	-- Numeric interval 
																	 	  	   p_endDate				DATE		   ,	-- 
																	 	  	   p_rowLimit				INT			   ,	-- 
																	 	  	   p_singleColumn   	VARCHAR(50)  ,	-- coulmn name to. include in where clause when date is not provided
																	 	  	   p_singleColumnWhere	VARCHAR(4000))	-- values to be passed to filter, can be coma seperted values as well   
SQL SECURITY INVOKER 
BEGIN 	
	DECLARE v_sqlSelect 	VARCHAR(1000);
	DECLARE v_sqlDelete 	VARCHAR(1000);
	DECLARE v_createSQ		VARCHAR(1000);
	DECLARE v_returnString  VARCHAR(1000);
	DECLARE v_endDate 		DATETIME;
	DECLARE v_startDate 	DATETIME;
	/* ######################################## 
	
			ERROR HANDLER DECLARATION
	
	########################################  */
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	BEGIN
           GET DIAGNOSTICS CONDITION 1
	     @errcode = Returned_SqlState, @msg = Message_text;
	END;
	/* ######################################## 
	
			INPUT PARAMETERS CONFIGS
	
	########################################  */
	SET @Id = 0;
	
	CREATE OR REPLACE TABLE HT_Visit (Id VARCHAR(32));
	CREATE OR REPLACE TABLE TOP_NRows (visitId VARCHAR(32));
	
	SET v_startDate = DATE_FORMAT(p_startDate, '%Y-%m-%e 00:00:00');
	IF p_startDate IS NOT NULL AND p_endDate IS NULL THEN	
		SET v_endDate   = CASE WHEN p_dateAttribute = 'Y' THEN DATE_ADD(p_startDate , INTERVAL p_attributeValue YEAR)
 						  		   WHEN p_dateAttribute = 'M' THEN DATE_ADD(p_startDate , INTERVAL p_attributeValue MONTH)
 						 		   WHEN p_dateAttribute = 'D' THEN DATE_ADD(p_startDate , INTERVAL p_attributeValue DAY)
 								   ELSE p_startDate END;
 		SET v_endDate = DATE_FORMAT(v_endDate, '%Y-%m-%e 23:59:59');
 	ELSE 
		SET v_endDate = DATE_FORMAT(p_endDate, '%Y-%m-%e 23:59:59');
	END IF;
	
	IF p_singleColumnWhere IS NOT NULL THEN 
		WHILE LENGTH(p_singleColumnWhere) > 0 
	    DO  
	     	 SET v_returnString := CONCAT('''', SUBSTRING_INDEX(p_singleColumnWhere,',',1), ''',');      
	  		 SET p_singleColumnWhere = SUBSTR(p_singleColumnWhere,LENGTH(SUBSTRING_INDEX(p_singleColumnWhere,',',1)) + 2 , LENGTH(p_singleColumnWhere));
	    END WHILE;
	SET p_singleColumnWhere := SUBSTR(v_returnString, 1, LENGTH(v_returnString) -1);  
	END IF;
		
	IF p_singleColumnWhere IS NULL AND p_rowLimit IS NOT NULL THEN 
		SET @createSQ :=  CONCAT('INSERT TOP_NRows SELECT uv.visitId FROM USR_Visit uv WHERE uv.createDate BETWEEN ''', v_startDate, ''' AND ''', v_endDate, ''' ORDER BY uv.createDate LIMIT ', p_rowLimit, ';'); 
	ELSEIF p_singleColumnWhere IS NULL AND p_rowLimit IS NULL THEN 
		SET @createSQ = CONCAT('INSERT TOP_NRows SELECT uv.visitId FROM USR_Visit uv WHERE uv.createDate BETWEEN ''', v_startDate, ''' AND ''', v_endDate, ''' ORDER BY uv.createDate;');
	ELSEIF	p_singleColumnWhere IS NOT NULL THEN 
		SET @createSQ = CONCAT('INSERT TOP_NRows SELECT uv.visitId FROM USR_Visit uv WHERE uv.visitId In (', p_singleColumnWhere, ');');
	END IF;	
	
	PREPARE v_createSQ FROM @createSQ;
	EXECUTE v_createSQ;
	
	SET @whereClause  = CONCAT(' uv.visitId In (SELECT visitId FROM TOP_NRows);');
	
/*  	SET @whereClause  = CASE WHEN p_singleColumnWhere IS NULL AND p_rowLimit IS NOT NULL THEN CONCAT(' uv.visitId In (SELECT visitId FROM TOP_NRows);')
							 WHEN p_singleColumnWhere IS NULL AND p_rowLimit IS NULL THEN CONCAT(' uv.createDate BETWEEN ''', v_startDate, ''' AND ''', v_endDate, ''';')
							 WHEN p_startDate IS NULL THEN CONCAT(' uv.visitId IN (', p_singleColumnWhere, ');') 
							 ELSE  '' END; 		  */					 
	/* ######################################## 
	
			LOOP TO PURGE TABLES
	
	########################################  */
	WHILE EXISTS(SELECT 1 FROM TOP_NRows) DO 
		SELECT Min(Id) , MAX(Id) INTO @minId,  @maxId FROM PUR_PurgeTables Where processFlag = 0; 
	
			Purger : LOOP
			
				BEGIN	
					SELECT `baseTable`,`query_select`, `query_delete`, 
							CASE WHEN foreignKeyCheck = 0 THEN '' ELSE 'SET FOREIGN_KEY_CHECKS = 0;' END disableSessionFK, 
							CASE WHEN foreignKeyCheck = 0 THEN '' ELSE 'SET FOREIGN_KEY_CHECKS = 1;' END reenablkeSessionFK
					INTO @Table, @sqlSelect, @sqlDelete, @disableSessionFK, @reenableSessionFK FROM PUR_PurgeTables WHERE Id = @minId; 
		/* 			BUILD SQL FOR CREATING HT_VISIT TABLE */ 			
					SET @sqlSelect := CONCAT(@sqlSelect, @whereClause);
		/* 			BUILD SQL FOR DELETE SQL */ 				 		
					SET @sqlDeleteMerge := CONCAT('IF EXISTS(SELECT 1 FROM HT_Visit) THEN', CHAR(10));
					SET @sqlDeleteMerge := CONCAT(@sqlDeleteMerge, @disableSessionFK, CHAR(10));
					SET @sqlDeleteMerge := CONCAT(@sqlDeleteMerge, @sqlDelete, ';', CHAR(10));
					SET @sqlDeleteMerge := CONCAT(@sqlDeleteMerge, @reenableSessionFK, CHAR(10));
					SET @sqlDeleteMerge := CONCAT(@sqlDeleteMerge, 'END IF;', CHAR(10));

		/* 			PARSE AND EXECUTE SEELECT AND DELETE SQL */
						PREPARE v_sqlSelect FROM @sqlSelect;
						PREPARE v_sqlDelete FROM @sqlDeleteMerge;
						EXECUTE v_sqlSelect;
						GET DIAGNOSTICS @rowCount = ROW_COUNT;
						SET @queryStartTime = NOW();
						EXECUTE v_sqlDelete;
		/* 			LOG PURGED ROWS 	  */			
							CALL usp_Log(@Table,NULL, (IFNULL(@rowCount, 0)), (IFNULL(@msg, '')),@sqlDeleteMerge,TIMESTAMPDIFF(SECOND,@queryStartTime, NOW()),NOW(),'Purger');
						SET @msg = NULL;
					SELECT  COUNT(1) TC, @minId, @sqlSelect, @sqlDeleteMerge, @whereClause FROM HT_Visit;	 		
					
					
					SET @minId := @minId + 1; 	 	
					IF 	@minId > @maxId THEN 
						LEAVE Purger;
					ELSE 
						ITERATE Purger;
					END IF; 
				END;
				
			END LOOP Purger;
		
		TRUNCATE TABLE TOP_NRows;
		UPDATE PUR_PurgeTables SET processFlag = 0;
		EXECUTE v_createSQ;
		
	END WHILE; 	
 	DROP TABLE IF EXISTS HT_Visit;
	DROP TABLE IF EXISTS TOP_NRows;
END; 

CREATE OR REPLACE DEFINER=CURRENT_USER PROCEDURE usp_Log(_p_tbl            VARCHAR(101),
												                        _p_Clm            VARCHAR(100),
												                        _p_rowsUpdated    INT,
												                        _p_errMsg         VARCHAR(1000),
												                        _p_query          VARCHAR(4000),
												                        _p_execTime       INT,
												                        _p_CreatedOn      DATETIME,
					                                                _p_feature       VARCHAR(50)
					                                                )
SQL SECURITY INVOKER 
BEGIN
      INSERT INTO LOG_DBMaintanenceLogs (`Table`, `Column`, RowsUpdated, ErrorMessage, `Query`, ExecutionTime, CreatedOn, Feature)
      SELECT _p_tbl, _p_Clm, _p_rowsUpdated, _p_errMsg, _p_query, _p_execTime, _p_CreatedOn, _p_feature;
END;		

//

DELIMITER ;
