** Script for SelectTopNRows command from SSMS  **/

---number of tables to iterate from MNG table--
declare @tablecount int = (select count(1) from  [dbo].[MNG_metadata_files_to_load])

---lenght to add to nvarchar()
declare @ColumnLengthToAdd int =  50
declare @ColumnDefaultLength nvarchar = '50'

declare @outercounter int = 1

----iterate the filelist
while @outercounter <=    @tablecount
BEGIN
DECLARE @Single_Table nvarchar(100)
SET @Single_Table =  (  SELECT [TargetTable] FROM
                     ( SELECT [TargetTable] 
                     ,row_number() over (order by (select (null))) as rn
				     FROM [dbo].[MNG_metadata_files_to_load] )A
					 WHERE RN = @outercounter )

PRINT(@Single_Table)

SELECT  quotename(COLUMN_NAME) as COLUMN_NAME, --, replace(quotename(COLUMN_NAME), '[ ', '[') as new, 
        row_number() over (order by (select (null))) as rn from INFORMATION_SCHEMA.COLUMNS
 where TABLE_NAME = @Single_Table 


 declare @InnerCounter int = 1
 declare @EndInnerCounter int =  (select count(1) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = @Single_Table )
 declare @CurrentColumn_WithoutBrackets nvarchar(150) 
  declare @CurrentColumn_WithBrackets nvarchar(150) 
 
 WHILE @InnerCounter <= @EndInnerCounter
 BEGIN
 SELECT @CurrentColumn_WithoutBrackets = ( SELECT REPLACE(REPLACE(COLUMN_NAME,'[',''),']','') FROM 
                          (SELECT  quotename(COLUMN_NAME) as COLUMN_NAME, --, replace(quotename(COLUMN_NAME), '[ ', '[') as new, 
                                  row_number() over (order by (select (null))) as rn from INFORMATION_SCHEMA.COLUMNS
                           where TABLE_NAME = @Single_Table 
                          ) A 
						  WHERE RN = @InnerCounter )

 SELECT @CurrentColumn_WithBrackets = ( SELECT COLUMN_NAME FROM 
                          (SELECT  quotename(COLUMN_NAME) as COLUMN_NAME, --, replace(quotename(COLUMN_NAME), '[ ', '[') as new, 
                                  row_number() over (order by (select (null))) as rn from INFORMATION_SCHEMA.COLUMNS
                           where TABLE_NAME = @Single_Table 
                          ) A 
						  WHERE RN = @InnerCounter )

--CHECK THE MAX LENGTH OF THIS CURRENT COLUMN
DECLARE @SQL nvarchar(max) = ''

		  declare @resultLength int

		  SET @SQL  = N' SELECT @LENGTH= MAX(LEN('+@CurrentColumn_WithBrackets+'))
             from '+@Single_Table+''

--catch the length in variable @resultLength
			 EXEC sp_executesql @SQL , N'@CurrentColumn_WithBrackets nvarchar(100), @Single_Table nvarchar(100), @LENGTH INT OUTPUT' , @CurrentColumn_WithBrackets=@CurrentColumn_WithBrackets
			, @Single_Table=@Single_Table, @LENGTH=@resultLength OUTPUT
    
--SELECT @resultLength AS [Columnmaxlenght]

---@NEWresultLength AS NVARCHAR VARIABLE , when above 4000 which is the maxim allowed the value will be 'MAX'--
DECLARE @NEWresultLength NVARCHAR(50) 
SELECT @NEWresultLength = CASE WHEN (@resultLength IS NULL OR @resultLength = 0) THEN  @ColumnDefaultLength
       WHEN (@resultLength +@ColumnLengthToAdd) > 4000  THEN 'MAX'
       ELSE CAST(@resultLength +50 AS NVARCHAR) END

	 set @Sql = N'  ALTER TABLE '+@Single_Table+'
	                ALTER COLUMN '+@CurrentColumn_WithBrackets+' NVARCHAR('+@NEWresultLength+')'
	           
		PRINT @SQL 
		EXECUTE (@sQL)


 SET @InnerCounter = @InnerCounter+1
 END

 SET @outercounter = @outercounter +1

END