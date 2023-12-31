USE ROLE SYSADMIN;
USE SCHEMA PLATFORM_DB.PLATFORM_APP;
CREATE OR REPLACE sequence ORGANIZATION_USAGE_load_run_SEQ start = 1 increment = 1;

CREATE OR REPLACE table PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS
(LoadName varchar ,Datekey varchar ,HashColumnKey varchar ,Active  INT default 0);


INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('REMAINING_BALANCE_DAILY','DATE','ORGANIZATION_NAME,CONTRACT_NUMBER,DATE,CURRENCY');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('RATE_SHEET_DAILY','DATE','ORGANIZATION_NAME,CONTRACT_NUMBER,DATE,ACCOUNT_NAME,REGION,SERVICE_TYPE');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('DATA_TRANSFER_HISTORY','USAGE_DATE','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,USAGE_DATE,TRANSFER_TYPE');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('SEARCH_OPTIMIZATION_HISTORY','USAGE_DATE','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,USAGE_DATE,TABLE_ID,SCHEMA_ID,DATABASE_ID');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('DATABASE_STORAGE_USAGE_HISTORY','USAGE_DATE','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,DATABASE_NAME,USAGE_DATE,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('AUTOMATIC_CLUSTERING_HISTORY','USAGE_DATE','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,TABLE_ID,SCHEMA_ID,DATABASE_ID, USAGE_DATE');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('STAGE_STORAGE_USAGE_HISTORY','USAGE_DATE','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,USAGE_DATE');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('PIPE_USAGE_HISTORY','USAGE_DATE','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,USAGE_DATE,PIPE_ID,PIPE_NAME');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('CONTRACT_ITEMS','START_DATE','ORGANIZATION_NAME,CONTRACT_NUMBER,START_DATE,CONTRACT_ITEM,AMOUNT,CONTRACT_MODIFIED_DATE');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('REPLICATION_USAGE_HISTORY','USAGE_DATE','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,USAGE_DATE,DATABASE_ID');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('DATA_TRANSFER_DAILY_HISTORY','USAGE_DATE','SERVICE_TYPE,ORGANIZATION_NAME,ACCOUNT_LOCATOR,USAGE_DATE,REGION,TB_TRANSFERED');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('USAGE_IN_CURRENCY_DAILY','USAGE_DATE','ORGANIZATION_NAME,CONTRACT_NUMBER,ACCOUNT_LOCATOR,REGION,USAGE_DATE,USAGE_TYPE,BALANCE_SOURCE');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('METERING_DAILY_HISTORY','USAGE_DATE','SERVICE_TYPE,ORGANIZATION_NAME, ACCOUNT_LOCATOR,USAGE_DATE,REGION');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('STORAGE_DAILY_HISTORY','USAGE_DATE','SERVICE_TYPE,ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,USAGE_DATE');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('WAREHOUSE_METERING_HISTORY','START_TIME','ORGANIZATION_NAME,ACCOUNT_LOCATOR,REGION,SERVICE_TYPE,WAREHOUSE_ID,START_TIME');
INSERT INTO PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_PROCESS (LoadName,Datekey,HashColumnKey) VALUES('QUERY_HISTORY','START_TIME','ACCOUNT_ID,QUERY_ID');


Update "PLATFORM_DB"."PLATFORM_APP"."ORGANIZATION_USAGE_LOAD_PROCESS" SET ACTIVE=1;

CREATE OR REPLACE TABLE ORGANIZATION_USAGE_LOAD_RUN (
	RUN_ID NUMBER(38,0) DEFAULT PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_load_run_SEQ.NEXTVAL COMMENT 'SYSTEM GENERATED SEQUENCE',
    ORGANIZATION_USAGE_LOAD VARCHAR,
    JOB_DESCRIPTION VARIANT,
    JOB_STARTED_TS TIMESTAMP_NTZ(9),
	JOB_COMPLETED_TS TIMESTAMP_NTZ(9)
);