----------- SQL STORED PROC
USE ROLE SYSADMIN  ;
USE DATABASE PLATFORM_DB;
USE SCHEMA PLATFORM_DB.ORGANIZATION_USAGE_HIST_ROUTINE;


CREATE OR REPLACE PROCEDURE PLATFORM_DB.ORGANIZATION_USAGE_HIST_ROUTINE.ORGANIZATION_USAGE_HISTORY_LOAD_SQL_PROC()
RETURNS ARRAY NOT NULL
LANGUAGE SQL
EXECUTE AS   OWNER
AS
$$



DECLARE
    JOB_NAME VARCHAR;
    JOB_ACTION VARCHAR;
    IS_ERROR VARCHAR DEFAULT '0';
    JOB_LOG_DESCRIPTION VARCHAR;
    SCHEMA_NAME_EXCEPTION EXCEPTION;
    LOADNAME        VARCHAR;
    DATEKEY         VARCHAR;
    HASHCOLUMNKEY   VARCHAR;
    CHECK_TABLE_SQL VARCHAR;
    LOAD_NAME_LIST VARCHAR DEFAULT '';
    LOG VARIANT;
    START_DATETIME DATETIME ;
    END_DATETIME   VARCHAR DEFAULT '';
    RES resultset;
    RUN_ID VARCHAR DEFAULT '';
    LOAD_SQL VARCHAR DEFAULT '';
    COLUMN_LIST VARCHAR DEFAULT '';
    HASHCOLUMNKEY_LIST VARCHAR DEFAULT '';
    DATE_SQL  VARCHAR DEFAULT '';
BEGIN
    --  Get the list of account usage load process name to load. LoadName  should match the view name in the snowflake.organization_usage.
    LET ORG_TABLE_NAMES CURSOR(p) FOR
        SELECT LOADNAME,DATEKEY,HASHCOLUMNKEY,REPLACE(HASHCOLUMNKEY,',', ',''|'',') AS HASHCOLUMNKEY_LIST, active FROM
        GBI_PLATFORM_DB.PLATFORM_APP.organization_usage_Load_Process  WHERE active=1;

    LOAD_NAME_LIST:='';
    OPEN ORG_TABLE_NAMES;
    FOR ACTIVE_ORG_TABLE_NAME IN ORG_TABLE_NAMES DO
        LOADNAME := ACTIVE_ORG_TABLE_NAME.LOADNAME;
        DATEKEY  := ACTIVE_ORG_TABLE_NAME.DATEKEY;
        HASHCOLUMNKEY := ACTIVE_ORG_TABLE_NAME.HASHCOLUMNKEY;
        HASHCOLUMNKEY_LIST:= ACTIVE_ORG_TABLE_NAME.HASHCOLUMNKEY_LIST ;

        --CHECK IF THE TABLE IS ALREADY CREATED IF NOT CREATE IT
        --CHECK_TABLE_AND_ADD_JS_PROC
        CHECK_TABLE_SQL :='CALL CHECK_TABLE_AND_ADD_JS_PROC('''||LOADNAME||''')';
        EXECUTE IMMEDIATE CHECK_TABLE_SQL;

        -- For the given materialized table ( load process name) gets START & END DATETIME
        DATE_SQL  := 'SELECT IFF(MAX('||DATEKEY ||')  IS NULL,TIMEADD(month,-36,CURRENT_TIMESTAMP()), TIMEADD(day,-1,MAX(' || DATEKEY ||')))  as start_datetime, TIMEADD(minute,+10,CURRENT_TIMESTAMP())  as end_datetime
        FROM GBI_PLATFORM_DB.ORGANIZATION_USAGE_HIST_APP.' ||LOADNAME ;
        RES  := (execute immediate :DATE_SQL);
        LET DATE_CUR CURSOR FOR RES;
        FOR DATE_CUR_VAR IN DATE_CUR DO
           START_DATETIME :=  DATE_CUR_VAR.START_DATETIME ;
           END_DATETIME   := DATE_CUR_VAR.END_DATETIME;

        END FOR;

        --MAKE AN ENTRY TO JOB TABLE ORGANIZATION_USAGE_LOAD_RUN
        INSERT INTO GBI_PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_RUN (ORGANIZATION_USAGE_LOAD,JOB_STARTED_TS,JOB_DESCRIPTION)
        VALUES(:LOADNAME,CURRENT_TIMESTAMP(),NULL);

        ---  GET THE MAX RUN ID
        LET  MAX_RUN_CURSOR CURSOR(p) FOR
            SELECT MAX(RUN_ID) as RUN_ID FROM  GBI_PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_RUN
            WHERE UPPER(ORGANIZATION_USAGE_LOAD)=? and JOB_COMPLETED_TS IS NULL;

        OPEN MAX_RUN_CURSOR USING (:LOADNAME);
        FOR RUNS IN MAX_RUN_CURSOR  DO
            RUN_ID := RUNS.RUN_ID;
        END FOR;
        CLOSE MAX_RUN_CURSOR;

        --Getting the list of column name for the given load process name
        LET COLUMN_CURSOR  CURSOR(p) FOR
            SELECT listagg (COLUMN_NAME,', ')  as COLUMN_LIST FROM  GBI_PLATFORM_DB.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME=?  AND TABLE_SCHEMA ='ORGANIZATION_USAGE_HIST_APP' AND COLUMN_NAME NOT IN ('DW_EVENT_SHK','RUN_ID','REGION_NAME');
        OPEN COLUMN_CURSOR USING (:LOADNAME);
        FOR COLUMN_ITEM IN COLUMN_CURSOR DO
                COLUMN_LIST := COLUMN_ITEM.COLUMN_LIST;
        END FOR;
        CLOSE COLUMN_CURSOR;
        -- BUILDING LOAD QUERY
        LOAD_SQL := '';
        IF (LOADNAME='QUERY_HISTORY') THEN
            COLUMN_LIST := REPLACE(COLUMN_LIST,'USAGE_DATE,','');
            COLUMN_LIST := REPLACE(COLUMN_LIST,'ORG_NAME,','');
            LOAD_SQL := 'INSERT INTO GBI_PLATFORM_DB.ORGANIZATION_USAGE_HIST_APP.'|| LOADNAME ||' (dw_event_shk ,RUN_ID,Usage_Date, ORG_NAME,' || COLUMN_LIST ||' ) ';
            LOAD_SQL := LOAD_SQL || '  with l_stg as (Select sha1_binary( concat( upper(current_account()) ,  UPPER(current_region()) ,' || '''IST'','||'''|'','||  HASHCOLUMNKEY_LIST ||')) as dw_event_shk,  ' || COLUMN_LIST  || ' from SNOWFLAKE.ORGANIZATION_USAGE.'|| LOADNAME || '  WHERE ' || DATEKEY  || ' >=  ''';
            LOAD_SQL := LOAD_SQL || START_DATETIME || ''' ),l_deduped as ( select *   from ( select  row_number() over( partition by dw_event_shk order by 1 ) as seq_no ,s.* ';
            LOAD_SQL := LOAD_SQL || '  from l_stg s ) where seq_no = 1 )  SELECT dw_event_shk ,  '|| RUN_ID || ',TO_DATE(START_TIME) ,'|| '''IST'' as ORG_NAME,' || COLUMN_LIST  || ' from l_deduped  where dw_event_shk not in ( ';
            LOAD_SQL := LOAD_SQL || ' select dw_event_shk from GBI_PLATFORM_DB.ORGANIZATION_USAGE_HIST_APP.'|| LOADNAME || ' WHERE '|| DATEKEY || ' >= ''' || START_DATETIME || '''  ) order by  ' || DATEKEY ;
        ELSE
            LOAD_SQL := 'INSERT INTO GBI_PLATFORM_DB.ORGANIZATION_USAGE_HIST_APP.'|| LOADNAME ||' (dw_event_shk ,RUN_ID, ' || COLUMN_LIST ||' ) ';
            LOAD_SQL := LOAD_SQL || '  with l_stg as (Select sha1_binary( concat( upper(current_account()) ,  UPPER(current_region()) ,'||  HASHCOLUMNKEY_LIST ||')) as dw_event_shk,  ' || COLUMN_LIST  || ' from SNOWFLAKE.ORGANIZATION_USAGE.'|| LOADNAME || '  WHERE ' || DATEKEY  || ' >=  ''';
            LOAD_SQL := LOAD_SQL || START_DATETIME || ''' ),l_deduped as ( select *   from ( select  row_number() over( partition by dw_event_shk order by 1 ) as seq_no ,s.* ';
            LOAD_SQL := LOAD_SQL || '  from l_stg s ) where seq_no = 1 )  SELECT dw_event_shk ,'|| RUN_ID || ',' || COLUMN_LIST  || ' from l_deduped  where dw_event_shk not in ( ';
            LOAD_SQL := LOAD_SQL || ' select dw_event_shk from GBI_PLATFORM_DB.ORGANIZATION_USAGE_HIST_APP.'|| LOADNAME || ' WHERE '|| DATEKEY || ' >= ''' || START_DATETIME || '''  ) order by  ' || DATEKEY ;
        END IF;

        --RETURN LOAD_SQL;
        EXECUTE IMMEDIATE LOAD_SQL;

        --CLOSE THE JOB RUN
         UPDATE GBI_PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_RUN SET JOB_COMPLETED_TS=CURRENT_TIMESTAMP(),JOB_DESCRIPTION=OBJECT_CONSTRUCT('is_error','0')  WHERE  RUN_ID= :RUN_ID ;


END FOR;
    CLOSE ORG_TABLE_NAMES;

    RETURN LOAD_SQL;


EXCEPTION

    WHEN SCHEMA_NAME_EXCEPTION  THEN
        IS_ERROR := '1';
        JOB_LOG_DESCRIPTION := 'ERROR: INVALID PARAMETERS' ;
        UPDATE GBI_PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_RUN SET JOB_COMPLETED_TS=CURRENT_TIMESTAMP(),JOB_DESCRIPTION=OBJECT_CONSTRUCT('is_error',JOB_LOG_DESCRIPTION)  WHERE  RUN_ID= :RUN_ID ;
        RETURN ARRAY_CONSTRUCT(:IS_ERROR,:JOB_LOG_DESCRIPTION)  ;

    WHEN OTHER THEN
        IS_ERROR := '1';
        JOB_LOG_DESCRIPTION := 'SQLCODE:' || SQLCODE || ' SQLERRM:' || SQLERRM || ' SQLSTATE:' ||SQLSTATE;
        UPDATE GBI_PLATFORM_DB.PLATFORM_APP.ORGANIZATION_USAGE_LOAD_RUN SET JOB_COMPLETED_TS=CURRENT_TIMESTAMP(),JOB_DESCRIPTION=OBJECT_CONSTRUCT('is_error',JOB_LOG_DESCRIPTION)  WHERE  RUN_ID= :RUN_ID ;
        RETURN ARRAY_CONSTRUCT(:IS_ERROR,:JOB_LOG_DESCRIPTION)  ;
END;

$$;


USE ROLE sysadmin;
USE DATABASE PLATFORM_DB;
USE SCHEMA PLATFORM_DB.ORGANIZATION_USAGE_HIST_ROUTINE;

 -----JS HELPER FUNCTION
CREATE OR REPLACE PROCEDURE CHECK_TABLE_AND_ADD_JS_PROC(LOADTABLE VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS   OWNER
AS
$$
var json_row = {};
var error_count = 0;
var start_datetime_v='' ;
var end_datetime_v ='';
var column_list ='';
var missing_table_sql='';
var add_table_sql='';
var RUN_ID='';
var is_table_exists='';
String.prototype.replaceAll = function(search, replace)
{
    //if replace is not sent, return original string otherwise it will
    //replace search string with 'undefined'.
    if (replace === undefined) {
        return this.toString();
    }
    return this.replace(new RegExp('[' + search + ']', 'g'), replace);
};
try {

    missing_table_sql =  "SELECT count(*) cnt FROM    PLATFORM_DB.INFORMATION_SCHEMA.TABLES Where TABLE_SCHEMA='ORGANIZATION_USAGE_HIST_APP' AND TABLE_NAME='" + LOADTABLE +"' ";
    json_row['missing_col_sql']=missing_table_sql;
    var col_QueryRes =snowflake.createStatement({sqlText: missing_table_sql}).execute();
    col_QueryRes.next();
    is_table_exists=col_QueryRes.getColumnValueAsString(1);
    if (is_table_exists=='0') { -- table doesn't exists
        add_table_sql="CREATE TABLE PLATFORM_DB.ORGANIZATION_USAGE_HIST_APP."+LOADTABLE +"  as SELECT sha1_binary( concat(    UPPER(current_account()))) AS DW_EVENT_SHK ,";
        add_table_sql=add_table_sql+ " current_region()   AS region_name, to_number(0,38,0)  as RUN_ID,s.* FROM SNOWFLAKE.ORGANIZATION_USAGE." +LOADTABLE +" s where 1<>1 "
        var col_create_QueryRes =snowflake.createStatement({ sqlText: add_table_sql}).execute();
        col_create_QueryRes.next();
        json_row['job_log_description']=col_create_QueryRes.getColumnValueAsString(1);
    }
}
catch(err) {
    json_row["is_error"] = 1;
    json_row["job_log_description"] = "Exception: " + err.message;
}
return json_row;
$$;
