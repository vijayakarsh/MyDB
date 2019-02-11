DROP TABLE IF EXISTS LOG_DBMaintanenceLogs;  
CREATE OR REPLACE TABLE LOG_DBMaintanenceLogs
        (
         Id               INT NOT NULL AUTO_INCREMENT,
        `Table`           VARCHAR(50),
        `Column`          VARCHAR(50),
         RowsUpdated      INT,
         ErrorMessage     VARCHAR(8000),
        `Query`           VARCHAR(8000),
         ExecutionTime    INT,
         CreatedOn        DATETIME,
         `Feature`        VARCHAR(50),
         PRIMARY KEY (Id)
    	  )
ENGINE = InnoDB;

CREATE OR REPLACE TABLE PUR_PurgeTables (Id 					INT AUTO_INCREMENT,
												 `baseTable`			VARCHAR(50),
												 `query_select`		VARCHAR(1000),
												 `query_delete`  	VARCHAR(1000),
												  processFlag 		BIT,
												  foreignKeyCheck	BIT,
												  PRIMARY KEY (Id)
												 )
												 ENGINE = InnoDB;
											 
DELIMITER //

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
 	
 	
 INSERT PUR_PurgeTables (`baseTable`, `query_select`, `query_delete`, processFlag, foreignKeyCheck)
 VALUES
 ('LOG_Hl7', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT lh.hl7LoggerId AS Id FROM LOG_Hl7 lh INNER JOIN USR_Visit uv ON lh.visitId = uv.visitId WHERE', 
  'DELETE lh FROM LOG_Hl7 lh INNER JOIN HT_Visit hv ON lh.hl7LoggerId = hv.Id', 0, 0),
 ('LOG_DisplayDeviceEventLog', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT ld.eventLogId AS Id FROM LOG_DisplayDeviceEventLog ld INNER JOIN USR_Visit uv ON ld.visitId = uv.visitId WHERE',
  'DELETE ld FROM LOG_DisplayDeviceEventLog ld INNER JOIN  HT_Visit hv ON ld.eventLogId = hv.Id', 0, 0), 
 ('LOG_InternetAccessLog', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT li.internetAccessLogId AS Id FROM LOG_InternetAccessLog li INNER JOIN USR_Visit uv ON li.visitId = uv.visitId WHERE',
  'DELETE li FROM LOG_InternetAccessLog li INNER JOIN HT_Visit hv ON li.internetAccessLogId = hv.Id', 0, 0),  
 ('CMN_GrantedPermission', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT grantedPermissionId AS Id FROM CMN_GrantedPermission cg INNER JOIN USR_Visit uv ON cg.visitId = uv.visitId WHERE',
  'DELETE cg FROM CMN_GrantedPermission cg INNER JOIN HT_Visit hv ON cg.grantedPermissionId = hv.Id', 0, 0), 
 ('LOG_ActivityLog', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT la.activityLogId As Id FROM LOG_ActivityLog la INNER JOIN USR_Visit uv ON la.visitId = uv.visitId WHERE',
  'DELETE la, lal FROM LOG_ActivityLog la INNER JOIN HT_Visit hv ON la.`activityLogId` = hv.Id LEFT JOIN LOG_ActivityLogData lal ON la.`activityLogId` = lal.`_activityLogId`', 0, 1),  
 ('LOG_InternetAccessLog', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT li.internetAccessLogId as Id FROM LOG_InternetSummary li INNER JOIN USR_Visit uv ON li.visitId = uv.visitId WHERE',
  'DELETE li FROM LOG_InternetSummary li INNER JOIN HT_Visit hv ON li.internetAccessLogId = hv.Id', 0, 0),   
 ('RPT_RetailRevenue', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT rr.retailTransactionId as Id FROM RPT_RetailRevenue rr INNER JOIN USR_Visit uv ON rr.visitId = uv.visitId WHERE',
  'DELETE rr FROM RPT_RetailRevenue rr INNER JOIN HT_Visit hv ON rr.retailTransactionId = hv.Id', 0, 0),   
 ('RPT2_EducationAssignment', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT rea.eduAssignmentId as Id FROM RPT2_EducationAssignment rea INNER JOIN USR_Visit uv ON rea._assignedVisitId = uv.visitId WHERE',
  'DELETE rea, uvl FROM RPT2_EducationAssignment rea INNER JOIN HT_Visit hv ON rea.eduAssignmentId = hv.Id LEFT JOIN USR_VisitLocationAlias uvl ON rea.`_visitLocationAliasId` = uvl.`_locationAliasId`', 0, 1),
 ('CMN_EducationOrders', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.educationOrderId as Id FROM CMN_EducationOrders a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE', 
  'DELETE ce, cea, ca,cl, mmo FROM CMN_EducationOrders ce INNER JOIN HT_Visit hv ON ce.educationOrderId = hv.Id LEFT JOIN CMN_EducationOrderAttributes cea ON ce.educationOrderId = cea._eduationOrderId LEFT JOIN CMN_Attribute ca ON cea._attributeId = 	ca.attributeId LEFT JOIN CMN_LanguageString cl ON ca._attributeTranslationId = cl.languageStringId LEFT JOIN MED2_MedicationEducationOrders mmo ON ce.educationOrderId = mmo.educationOrderId', 0, 1),
 ('CMN_EducationSelection', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.eduSelectionId as Id FROM CMN_EducationSelection a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE', 
  'DELETE ce FROM CMN_EducationSelection ce INNER JOIN HT_Visit hv ON ce.eduSelectionId = hv.Id', 0, 0),
 ('CMN_Goal', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.id as Id FROM CMN_Goal a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE cg FROM CMN_Goal cg INNER JOIN HT_Visit hv ON cg.Id = hv.Id', 0, 0),
 ('CMN_ImageAnnotation', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.imageAnnotationId as Id FROM CMN_ImageAnnotation a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE ci FROM CMN_ImageAnnotation ci INNER JOIN HT_Visit hv ON ci.imageAnnotationId = hv.Id', 0, 0),
 ('CUR_CarePlanStatus', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.carePlanStatusId as Id FROM CUR_CarePlanStatus a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE cc FROM CUR_CarePlanStatus cc INNER JOIN HT_Visit hv ON cc.carePlanStatusId = hv.Id', 0, 0),
 ('IBX_Item', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.inboxItemId as Id FROM IBX_Item a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE', 
  'DELETE ii, ic , ip FROM IBX_Item ii INNER JOIN HT_Visit hv ON ii.inboxItemId = hv.Id LEFT JOIN IBX_CurriculumItem ic on ii.inboxItemId = ic.inboxItemId LEFT JOIN IBX_PrescribedOrderItem ip ON ii.inboxItemId = ip.inboxItemId', 0, 1), 
 ('IBX_VisitStatus', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.inboxVisitStatusId as Id FROM IBX_VisitStatus a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE', 
  'DELETE iv FROM IBX_VisitStatus iv INNER JOIN HT_Visit hv ON iv.inboxVisitStatusId = hv.Id', 0, 0),
 ('LOG_WebServiceClientLog', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.wsLoggerId as Id FROM LOG_WebServiceClientLog a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE', 
  'DELETE lg FROM LOG_WebServiceClientLog lg INNER JOIN HT_Visit hv ON hv.Id = lg.wsLoggerId', 0, 0),
 ('MED2_MedicationOrders', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.medicationOrderId as Id FROM MED2_MedicationOrders a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE mmo, mm, mma, mmc, mmh FROM MED2_MedicationOrderSets mmo INNER JOIN HT_Visit hv ON mmo.`medicationOrderSetId` = hv.Id LEFT JOIN MED2_MedicationOrders mm ON mmo.`medicationOrderSetId` = mm._orderSetId LEFT JOIN MED2_MedicationOrderAttributes mma ON mm.medicationOrderId = mma._medicationOrderId LEFT JOIN MED2_MedicationOrderComponents mmc ON mm.`medicationOrderId` = mmc.`_medicationOrderId` LEFT JOIN MED2_MedicationOrderHistory mmh ON mm.`medicationOrderId` =. mmh.`_medicationOrderId`', 0, 1),
 ('PRI_UserPriority', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.id as Id FROM PRI_UserPriority a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE pu FROM PRI_UserPriority pu INNER JOIN HT_Visit hv ON hv.Id = pu.id', 0, 0),
 ('RPT2_EducationAssignment', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.eduAssignmentId as Id FROM RPT2_EducationAssignment a INNER JOIN USR_Visit uv ON a._assignedVisitId=uv.visitId WHERE',
  'DELETE rea FROM RPT2_EducationAssignment rea INNER JOIN HT_Visit hv ON hv.Id = rea.eduAssignmentId', 0, 0),
 ('RPT_NotificationLastActivity', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.logId as Id FROM RPT_NotificationLastActivity a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE rna FROM RPT_NotificationLastActivity rna INNER JOIN HT_Visit hv ON hv.Id = rna.logId', 0, 0),
 ('RPT_NotificationUsage', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.notificationLogId as Id FROM RPT_NotificationUsage a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE rn FROM RPT_NotificationUsage rn INNER JOIN HT_Visit hv ON hv.Id = rn.notificationLogId', 0, 0),
 ('RTL_RetailOrder', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.retailOrderId as Id FROM RTL_RetailOrder a INNER JOIN USR_Visit uv ON a._visitId = uv.visitId WHERE', 
  'DELETE rr, roi, rrt FROM RTL_RetailOrder rr INNER JOIN HT_Visit hv ON rr.`retailOrderId` = hv.Id LEFT JOIN RTL_OrderItem roi ON rr.`retailOrderId` = roi.`_retailOrderId` LEFT JOIN RTL_RetailTransaction rrt ON rr.`retailOrderId` = rrt.`_retailOrderId`', 0, 1),
 ('SR_ServiceRequest', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.serviceRequestId as Id FROM SR_ServiceRequest a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE', 
  'DELETE ss,sa,sc,sn,srs,sst  FROM SR_ServiceRequest ss INNER JOIN HT_Visit hv ON ss.serviceRequestId = hv.Id LEFT JOIN SR_Assignee sa ON ss.serviceRequestId = sa._serviceRequestId LEFT JOIN SR_Comment sc ON ss.serviceRequestId = sc._serviceRequestId LEFT JOIN SR_Notification sn ON ss.serviceRequestId = sn._serviceRequestId LEFT JOIN SR_RemoteSystemInformation srs ON ss.serviceRequestId = srs._serviceRequestId LEFT JOIN SR_Status sst ON ss.serviceRequestId = sst._serviceRequestId', 0, 1),
 ('STA_StaffRoundingVisit', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.roundingVisitId as Id FROM STA_StaffRoundingVisit a INNER JOIN USR_Visit uv ON a._roundingVisitId=uv.visitId WHERE',
  'DELETE ssr FROM STA_StaffRoundingVisit ssr INNER JOIN HT_Visit hv ON ssr.roundingVisitId = hv.Id', 0, 0),
 ('USR_PatientDischargeInstructions', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientDischargeId as Id FROM USR_PatientDischargeInstructions a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE upd FROM USR_PatientDischargeInstructions upd INNER JOIN HT_Visit hv ON upd.`patientDischargeId` = hv.Id', 0, 0),
 ('USR_PatientLabResult', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientLabId as Id FROM USR_PatientLabResult a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE upr FROM USR_PatientLabResult upr INNER JOIN HT_Visit hv ON upr.`patientLabId` = hv.Id', 0, 0),
 ('USR_PatientProblem', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientProblemId as Id FROM USR_PatientProblem a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE up FROM USR_PatientProblem up INNER JOIN HT_Visit hv ON up.`patientProblemId` = hv.Id', 0, 0),
 ('USR_VisitCodedData', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.visitCodedDataId as Id FROM USR_VisitCodedData a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE uv FROM USR_VisitCodedData uv INNER JOIN HT_Visit hv ON uv.`visitCodedDataId` = hv.Id', 0, 0),
 ('WHT_PatientContact', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientContactId as Id FROM WHT_PatientContact a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE wp FROM WHT_PatientContact wp INNER JOIN HT_Visit hv ON wp.`patientContactId` = hv.Id', 0, 0),
 ('WHT_PatientEntry', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientEntryId as Id FROM WHT_PatientEntry a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE wp FROM WHT_PatientEntry wp INNER JOIN HT_Visit hv ON wp.`patientEntryId` = hv.Id', 0, 0),
 ('WHT_PatientGoal', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientGoalId as Id FROM WHT_PatientGoal a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE wp FROM WHT_PatientGoal wp INNER JOIN HT_Visit hv ON wp.`patientGoalId` = hv.Id', 0, 0),
 ('WHT_PatientQuestion', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientQuestionId as Id FROM WHT_PatientQuestion a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE wp FROM WHT_PatientQuestion wp INNER JOIN HT_Visit hv ON wp.`patientQuestionId` = hv.Id', 0, 0),
 ('WHT_VisitEvents', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a._calendarEventId as Id FROM WHT_VisitEvents a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE wv FROM WHT_VisitEvents wv INNER JOIN HT_Visit hv ON wv.`_calendarEventId` = hv.Id', 0, 0),
 ('MED2_PatientMedications', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT a.patientMedicationId as Id FROM MED2_PatientMedications a INNER JOIN USR_Visit uv ON a._visitId=uv.visitId WHERE',
  'DELETE mp,mme,mmh,mmc FROM MED2_PatientMedications mp INNER JOIN HT_Visit hv ON mp.patientMedicationId = hv.Id LEFT JOIN MED2_MedicationEducationOrders mme ON mp.patientMedicationId =  mme._patientMedicationId LEFT JOIN MED2_MedicationHistory mmh ON mp.patientMedicationId = mmh._patientMedicationId LEFT JOIN MED2_MedicationOrderComponents mmc ON mp.patientMedicationId = mmc._patientMedicationId', 0, 1),
 ('MYGWN_OrderInformation', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT mo.orderId as Id FROM MYGWN_OrderInformation mo INNER JOIN USR_Visit uv ON mo._visitId = uv.visitId WHERE',
  'DELETE mo FROM MYGWN_OrderInformation mo INNER JOIN HT_Visit hv ON mo.`orderId` = hv.Id', 0, 0),
 ('USR_VisitLocationAlias', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT uva.visitLocationAliasId as Id FROM USR_Visit uv INNER JOIN USR_VisitLocationAlias uva ON uv.visitId = uva._visitId WHERE',
  'DELETE uva FROM USR_VisitLocationAlias uva INNER JOIN HT_Visit hv ON uva.visitLocationAliasId = hv.Id', 0, 0), 
 ('MMS_MediaSession', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT ms.mediaSessionId as Id FROM MMS_MediaSession ms INNER JOIN USR_Visit uv ON ms._assignedVisitId = uv.visitId WHERE',
  'DELETE ms, cm, msp FROM MMS_MediaSession ms INNER JOIN HT_Visit hv ON ms.mediaSessionId = hv.Id LEFT JOIN CUR_MediaEduUnit cm ON ms.mediaSessionId = cm._mediaSessionId LEFT JOIN MMS_SessionPlaylistItem msp ON ms.mediaSessionId = msp._mediaSessionId', 0, 1),
 ('CUR_PrescribedOrder', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT cp.prescribedOrderId as Id FROM CUR_PrescribedOrder cp INNER JOIN USR_Visit uv ON cp._visitId = uv.visitId WHERE',
  'DELETE cp, co, ip, cr, ca, cls  FROM CUR_PrescribedOrder cp INNER JOIN HT_Visit hv ON cp.prescribedOrderId = hv.Id LEFT JOIN CUR_OrderSession co ON cp.prescribedOrderId = co._rxOrderId LEFT JOIN  	IBX_PrescribedOrderItem ip ON cp.`prescribedOrderId` = ip.`_prescribedOrderId` LEFT JOIN CUR_RxAttributes cr ON cp.`prescribedOrderId` = cr.`_orderId` LEFT JOIN CMN_Attribute ca ON 
 	cr.`_attributeId` = ca.`attributeId` LEFT JOIN CMN_LanguageString cls ON ca.`_attributeTranslationId` = cls.`languageStringId`', 0, 1),
 ('CUR_OrderSet', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT co.orderSetId as Id FROM CUR_OrderSet co INNER JOIN USR_Visit uv ON co._visitId = uv.visitId WHERE',
  'DELETE co FROM CUR_OrderSet co INNER JOIN HT_Visit hv ON co.orderSetId = hv.Id', 0, 0),
 ('CUR_EduUnitInstance', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT ce.unitInstanceId as Id FROM CUR_CurriculumInstance cc INNER JOIN USR_Visit uv ON cc._visitId = uv.visitId INNER JOIN  CUR_EduUnitInstance ce ON cc.curriculumInstanceId = ce.unitInstanceId WHERE',
  'DELETE ce, cc, cm, cq, cs FROM CUR_EduUnitInstance ce INNER JOIN HT_Visit hv ON ce.unitInstanceId = hv.Id LEFT JOIN CUR_CustomContentUnit cc ON ce.unitInstanceId = cc._unitId LEFT JOIN CUR_MediaEduUnit cm ON ce.unitInstanceId = cm.mediaUnitId LEFT JOIN CUR_QuestionUnits cq ON ce.unitInstanceId = cq._unitId LEFT JOIN CUR_SubCurriulumUnit cs ON ce.unitInstanceId = cs._unitId', 0, 1), 
 ('CUR_CurriculumInstance', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT ci.curriculumInstanceId as Id FROM CUR_CurriculumInstance ci INNER JOIN USR_Visit uv ON ci._visitId = uv.visitId WHERE',
 'DELETE ci, cc, ce, co, cs, cca, ca, cls, ct, ctd FROM CUR_CurriculumInstance ci INNER JOIN HT_Visit hv ON ci.curriculumInstanceId = hv.Id
  LEFT JOIN CUR_CarePlanStatus cc ON ci.curriculumInstanceId = cc._curriculumId LEFT JOIN CUR_EduUnitInstance ce ON ci.curriculumInstanceId = ce._curriculumId LEFT JOIN CUR_OrderSession co ON   ci.curriculumInstanceId = co._curriculumSessionId LEFT JOIN CUR_SubCurriulumUnit cs ON ci.curriculumInstanceId = cs._subCurriculumInstanceId LEFT JOIN CUR_CurriculumInstanceAttributes cca ON ci.curriculumInstanceId = cca._curriculumInstanceId LEFT JOIN CMN_Attribute ca ON cca._attributeId = ca.attributeId LEFT JOIN CMN_LanguageString cls ON ca._attributeTranslationId =  cls.languageStringId LEFT JOIN CMN_Translations ct On cls.languageStringId = ct._languageStringId LEFT JOIN CMN_TranslationsDirty ctd ON cls.languageStringId = ctd._languageStringId', 0, 1),
 ('USR_Visit', 'CREATE OR REPLACE TABLE HT_Visit AS SELECT uv.visitId as Id FROM USR_Visit uv WHERE',
  'DELETE uv, cv, ca, cls, ct, ctd FROM USR_Visit uv INNER JOIN HT_Visit hv ON uv.visitId = hv.Id LEFT JOIN CMN_VisitAttributes cv ON uv.visitId = cv._visitId LEFT JOIN CMN_Attribute ca ON 			cv._attributeId = ca.attributeId LEFT JOIN CMN_LanguageString cls ON ca._attributeTranslationId = cls.languageStringId LEFT JOIN CMN_Translations ct On 
   cls.languageStringId = ct._languageStringId LEFT JOIN CMN_TranslationsDirty ctd ON cls.languageStringId = ctd._languageStringId', 0, 1); 
