/****** Object:  StoredProcedure [dbo].[sp_DynamicMerge]    Script Date: 5/26/2021 4:08:24 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[sp_DynamicMerge] @table [NVARCHAR](150) AS
BEGIN 

SET NOCOUNT ON

BEGIN TRY
BEGIN TRANSACTION
/****************************************************************************************************************/
/****************************************************************************************************************/
/******************************* Dynamic Merge Script fot AZURE SQL ********************************/
/*          
			The script requires primary keys defined on the target table in order to work 
*****************************************************************************************************/
/****************************************************************************************************************/

-- CONFIGURATION ------------------------------------------------------------------
--DECLARE @TABLE VARCHAR(100) = 'JobsDataAGG'
DECLARE @return int,
	   @sql varchar(max) = '',
	   @list varchar(max) = '',
	   @sourcePrefix varchar(max) = 'STG_',
	   @targetPrefix varchar(max) = 'DWH_',
	   @schema varchar(max) = 'dbo'

DECLARE  @DELTA varchar(max) = '[' + @sourcePrefix + @table + ']',
		 @HISTORY varchar(max) = '[' + @targetPrefix + @table + ']'

DECLARE @UPDATE varchar(max) = '',
        @SET varchar(max) = '',
		@SETLIST varchar(max) = '',
		@JOINLIST varchar(max) = '',
        @FROM varchar(max) = '',
	    @JOIN  varchar(max) = '',
	    @ON varchar(max) = '',
		@INSERT varchar(max) = '',
		@WHERE varchar(max) = ''



------------------------BUILD THE UPDATE STATEMENT--------------------------------------------------------------------------------

SET @UPDATE = 'UPDATE ' + @HISTORY         --+ ' AS T '
SET @FROM   = char(10) + ' FROM [' + @sourcePrefix + @table + ']' + ' AS S '


-- SET the @SETLIST ----------------------------------------------------------------------------------


   SET @SETLIST= ' SET ' +(SELECT   STRING_AGG('[' +CAST([name] AS VARCHAR(MAX))+'] = S.['+CAST([name] AS VARCHAR(MAX))+ ']', ', '+CHAR(10)) 
    FROM sys.columns
WHERE object_id = object_id(@targetPrefix + @table)
 --AND [name] <> 'DateRowInserted'
-- don't update primary keys
AND [name] NOT IN (SELECT [column_name]
                     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
                          INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
                    WHERE pk.TABLE_NAME = @targetPrefix + @table
                    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND c.TABLE_NAME = pk.TABLE_NAME
                    AND c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME) )

			
		
------- Get the join list----  (Primary keys)-------------------------------------------------------------------
   SET @JOINLIST= (SELECT   STRING_AGG('T.['+c.COLUMN_NAME+'] = S.['+c.COLUMN_NAME+ ']', ' AND '+ CHAR(10))
  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk,
       INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
 WHERE pk.TABLE_NAME = @targetPrefix + @table
   AND CONSTRAINT_TYPE = 'PRIMARY KEY'
   AND c.TABLE_NAME = pk.TABLE_NAME
   AND c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME )



 




--------------------------------INSERT STATEMENT------------------------------------
---INSERT WHEN THE ROWS ARE NEW-----------------------------------------------------

SET @INSERT   =  ' INSERT INTO ' + @HISTORY
                  +CHAR(10) + ' SELECT S.* FROM ' +  @DELTA + ' AS S'
				  +CHAR(10) + ' LEFT JOIN ' +@HISTORY + ' AS T ON '
				  +CHAR(10) + @JOINLIST

SET @WHERE      = 'WHERE ' +  (SELECT   STRING_AGG('T.['+c.COLUMN_NAME+'] IS NULL ', ' OR '+ CHAR(10))
  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk,
       INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
 WHERE pk.TABLE_NAME = @targetPrefix + @table
   AND CONSTRAINT_TYPE = 'PRIMARY KEY'
   AND c.TABLE_NAME = pk.TABLE_NAME
   AND c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME )
  

   

-- EXEC SQL ------------------------------------------------------------------------------------

SET @sql =             @UPDATE + @SETLIST + @FROM +' JOIN '+@HISTORY + ' AS T ON '  +@JOINLIST + ';'
				        +CHAR(10) + @INSERT
						+CHAR(10) + @WHERE

--print  @sql 


--- @SQL IS TOO LONG AND WILL TRUNCATE SO WE PRINT IT IN PARTS----
declare @i int = 1
while Exists(Select(Substring(@SQL,@i,4000))) and (@i < LEN(@SQL))
begin
     print Substring(@SQL,@i,4000)
     set @i = @i+4000
end


EXECUTE (@SQL)

  commit transaction;

--EXEC sp_executesql @sql;

END TRY

begin catch

if @@trancount > 0 rollback transaction;

throw

end catch
 


END