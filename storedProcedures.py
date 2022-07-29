# import json
import pg8000 as pg
# from pprint import pprint


def getconnection(database, host, port, user, password):
    conn = None
    try:
        conn = pg.connect(database=database, host=host, port=port,
                          user=user, password=password, ssl=True)

        print("Connection to redshift made successfully...")
    except Exception as err:
        print(err)
    return conn


def runquery(conn, query):
    """
    Just run a query given a connection
    """
    curr = conn.cursor()
    curr.execute(query)
    print("Query executed...")
    # for row in curr.fetchall():
    #     pprint(row)
    conn.commit()
    print("Commmited...")
    return None


if __name__ == '__main__':
    config = {
        "database": "dev",
        "host": "ensign-de-cluster.coovtxddmae6.us-east-1.redshift.amazonaws.com:5439/dev",
        "port": 5439,
        "user": 'awsuser',
        "password": 'Ensign123'
    }

    sample_query = """
    USE [RoxDbTest]
    GO

    /****** Object:  StoredProcedure [dbo].[usp_insert_dbms_to_stg_run_log]    Script Date: 7/28/2022 3:10:01 AM ******/
    SET ANSI_NULLS ON
    GO

    SET QUOTED_IDENTIFIER ON
    GO

    CREATE PROCEDURE [dbo].[usp_insert_dbms_to_stg_run_log]
        @master_stage_run_info_id int,
        @source_id int,
        @batch_id int,
        @object_name varchar(30),
        @glue_job_run_id varchar(100),      
        @o_dbms_to_stg_run_info_id int output,
        @o_last_incremental_id int output,
        @o_last_incremental_date datetime output,
        @o_message nvarchar(max) output
    AS
    BEGIN
        SET NOCOUNT ON;

        DECLARE @v_master_stage_run_info_id int = @master_stage_run_info_id,
                @v_source_id int = @source_id,
                @v_source_name varchar(30),
                @v_object_name varchar(30) = @object_name,
                @v_glue_job_run_id varchar(100) = @glue_job_run_id,             
                @v_batch_id int = @batch_id,
                @v_elh_created_utc_ts datetime2,
                @v_elh_created_by nvarchar(50),
                @v_elh_updated_utc_ts datetime2,
                @v_elh_updated_by nvarchar(50),
                @v_execution_start_datetime datetime2

        SELECT @v_execution_start_datetime = SYSDATETIME(),
                @v_elh_created_utc_ts = SYSDATETIME(),
                    @v_elh_created_by = USER_NAME(),
                @v_elh_updated_utc_ts = SYSDATETIME(),
                @v_elh_updated_by = USER_NAME()

            SELECT 
                @o_last_incremental_id = MAX(last_incremental_id), 
                @o_last_incremental_date = MAX(last_incremental_date) 
            FROM dbo.dbms_to_stg_run_log
            WHERE source_id = @source_id
            AND LOWER([object_name]) = LOWER(@object_name)
            AND [status] = 'Successful'

            SELECT 
                @v_source_name = source_name 
            FROM dbo.source_info 
            WHERE source_id = @source_id

        IF EXISTS (SELECT 1 FROM dbo.dbms_to_stg_run_log
                WHERE source_id = @v_source_id                
                    AND [object_name] = @v_object_name
                            AND [status] = 'InProgress')
        BEGIN
            --SET @o_dbms_to_stg_run_info_id = NULL;
            SELECT @o_dbms_to_stg_run_info_id = dbms_to_stg_run_info_id 
            FROM dbo.dbms_to_stg_run_log
            WHERE source_id = @v_source_id AND [object_name] = @v_object_name AND [status] = 'InProgress'
            --SET @o_last_incremental_id = NULL;
            SET @o_message = 'Job already InProgress or No. of attempts has exceeded the limit (3). Please try again later'  
        END
        ELSE 
        BEGIN
            INSERT INTO dbo.dbms_to_stg_run_log
                (master_stage_run_info_id,source_id,batch_id,[object_name],execution_start_datetime,[status],attempts,glue_job_run_id,elh_created_utc_ts,elh_created_by,elh_updated_utc_ts,elh_updated_by)
            VALUES
                (@v_master_stage_run_info_id, @v_source_id,@batch_id,@v_object_name,@v_execution_start_datetime,'InProgress',1,@v_glue_job_run_id,@v_elh_created_utc_ts,@v_elh_created_by,@v_elh_updated_utc_ts,@v_elh_updated_by)
        
            SET @o_dbms_to_stg_run_info_id = SCOPE_IDENTITY()     
            SET @o_message = 'New record has been inserted in dbo.dbms_to_stg_run_log for ' + @v_source_name + ' with dbms_to_stg_run_info_id = ' + CAST(@o_dbms_to_stg_run_info_id as nvarchar(max)) + '.';
        END

        --RETURN @o_dbms_to_stg_run_info_id
        SET @o_last_incremental_id = ISNULL(@o_last_incremental_id,0)
        --RETURN ISNULL(@o_last_incremental_date,GETDATE()-2)
        SET @o_last_incremental_date = ISNULL(@o_last_incremental_date,GETDATE()-2)
        --RETURN @o_message      
    END
    GO
    """

    sample_query_2 = """
    SELECT * FROM PG_TABLE_DEF
    WHERE
    schemaname = 'public';
    """
    conn = getconnection(config['database'], config['host'],
                         config['port'], config['user'], config['password'])
    runquery(conn, sample_query_2)
