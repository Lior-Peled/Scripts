/****** Object:  StoredProcedure [sync].[CreateScript]    Script Date: 7/13/2021 5:57:54 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [sync].[CreateScript] @TableID [int] AS

BEGIN

-- 1. Drop & Create table with Data Tracking data from originator DB
-- 2. Drop & Create stored procedure to transfer log data to work table

DECLARE
	@TableSchemaR nvarchar(20),
	@TableNameR nvarchar(100),
	@MatchedColumns nvarchar(4000),
	@OriginatorKeyColumns nvarchar(4000),
	@log_table_name nvarchar(200), 
	@table_name nvarchar(200), 
	@obj_id int,
	@NL NVARCHAR(10),
	@spName nvarchar(200),
	@SQL nvarchar(MAX),
	@ColumnList NVARCHAR(MAX),
	@OnStatement NVARCHAR(MAX),
	@SetStatement NVARCHAR(MAX),
	@InsertCommand NVARCHAR(MAX),
	@DoUpdateCommand bit = 1,
	@UpdateCommand NVARCHAR(MAX),
	@DeleteCommand NVARCHAR(MAX),
	@PageSize bigint = 100000

-- -----------------------------------------
-- Sync objects must be generated if Data Tracking is enabled only
-- Check if Data Tracking enabled
-- -----------------------------------------
IF (select [isDataTrackingEnabled] from [sync].[ManagementInfo] where TableID = @TableID) = 1 
BEGIN

-- -----------------------------------------
-- Table with Data Tracking data
-- -----------------------------------------

SET @NL = CHAR(13) + CHAR(10)

SELECT 
	@TableSchemaR = [TableSchemaRecipient], 
	@TableNameR = [TableNameRecipient],
	@table_name = QUOTENAME( TableSchemaRecipient ) + '.' + QUOTENAME( TableNameRecipient ),
	@spName = QUOTENAME( 'sync' ) + '.' + QUOTENAME('Incremental_' + TableNameRecipient )
FROM 
	[sync].[Management] 
WHERE [TableID] = @TableID

select 
	@MatchedColumns = [MatchedColumns],
	@OriginatorKeyColumns = [OriginatorKeyColumns]
from 
	[sync].[ManagementInfo] 
where [TableID] = @TableID

-- Fix: Check if key columns are equals to matched columns
-- If yes, then UpdateCommand is not nessesary in sync procedure
SET @DoUpdateCommand = (
	SELECT CASE WHEN count(*) > 0 THEN 1 ELSE 0 END FROM (
	SELECT value FROM STRING_SPLIT(@MatchedColumns, ';')
	EXCEPT
	SELECT value FROM STRING_SPLIT(@OriginatorKeyColumns, ';')) a )

SELECT 
      @log_table_name = QUOTENAME( 'sync' ) + '.' + QUOTENAME('Log_' + o.name ), 
	  @obj_id = o.[object_id]
FROM sys.objects o 
	JOIN sys.schemas s ON o.[schema_id] = s.[schema_id]
WHERE s.name + '.' + QUOTENAME( o.name ) = @TableSchemaR + '.' + QUOTENAME( @TableNameR )
    AND o.[type] = 'U'
    AND o.is_ms_shipped = 0

SET @SQL = 
	'IF OBJECT_ID(N' + '''' + @log_table_name + '''' + ', N' + '''' + 'U' + '''' + ') IS NOT NULL ' +
	'DROP TABLE ' + @log_table_name; 

EXEC sys.sp_executesql @SQL
-- PRINT 'SQL = ' + isnull(@SQL, '')

SET @SQL = (
	SELECT
	'CREATE TABLE ' + @log_table_name +
	'( ' +
	STRING_AGG(
		QUOTENAME(c.name) + ' ' + 
		QUOTENAME(UPPER(tp.name)) + 
			CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text')
					THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
					WHEN tp.name IN ('nvarchar', 'nchar', 'ntext')
					THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
					WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset') 
					THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
					WHEN tp.name IN ('numeric', 'decimal')
					THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
				ELSE ''
			END +
			CASE WHEN kc.name is null THEN ' NULL' ELSE ' NOT NULL' END, ', ') WITHIN GROUP (ORDER BY c.column_id ASC) + ', ' +
			'[SYS_CHANGE_VERSION] [BIGINT] NOT NULL, ' +
			'[SYS_CHANGE_OPERATION] nchar(1) NOT NULL,' +
			'[RowNum] [BIGINT] NOT NULL' +
			') WITH ( DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX )' 
	FROM sys.types AS tp INNER JOIN
		sys.columns AS c ON tp.user_type_id = c.user_type_id LEFT OUTER JOIN
		sys.key_constraints AS kc INNER JOIN
		sys.index_columns AS ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id AND kc.type = 'PK' ON c.object_id = ic.object_id AND c.column_id = ic.column_id
	WHERE 
	c.[object_id] = @obj_id 
)

-- PRINT 'SQL = ' + isnull(@SQL, '')
EXEC sys.sp_executesql @SQL

-- -----------------------------------------
-- Create Sync Stored Procedure
-- -----------------------------------------

IF EXISTS (SELECT 1 FROM sys.objects WHERE type = 'P' AND OBJECT_ID = OBJECT_ID(@spName))
BEGIN
	SELECT @SQL = 'DROP PROCEDURE ' + @spName
	EXEC sys.sp_executesql @SQL
END

SELECT @ColumnList = 
	STRING_AGG(QUOTENAME(c.COLUMN_NAME), ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION ASC)
FROM INFORMATION_SCHEMA.COLUMNS c INNER JOIN 
	(select value COLUMN_NAME from STRING_SPLIT(@MatchedColumns, ';')) m ON c.COLUMN_NAME = m.COLUMN_NAME
WHERE TABLE_NAME = @TableNameR AND TABLE_SCHEMA = @TableSchemaR
-- PRINT 'ColumnList = ' + @ColumnList

SET @InsertCommand = @NL +
	'WHILE @PageNumber <= @PageCount' + @NL +
	'BEGIN' + @NL +
	'INSERT INTO ' + @table_name + ' ' + @NL +
	'(' + @ColumnList + ')' + @NL +
	'SELECT TOP (@pagesize) ' + @NL + @ColumnList + @NL +
	'FROM ' + @log_table_name + ' c' + @NL +
	'WHERE c.SYS_CHANGE_OPERATION = ' + '''' + 'I' + ''''  + @NL +
	'and (@PageNumber - 1) * @PageSize < RowNum' + @NL + @NL +
	'SET @PageNumber +=1'+ @NL +
	'END' 
-- PRINT 'InsertCommand = ' + @InsertCommand

SELECT @OnStatement = 
	STRING_AGG('a.' + QUOTENAME(c.COLUMN_NAME) + ' = b.' + QUOTENAME(c.COLUMN_NAME), ' and ') WITHIN GROUP (ORDER BY ORDINAL_POSITION ASC)
FROM INFORMATION_SCHEMA.COLUMNS c INNER JOIN 
	(select value COLUMN_NAME from STRING_SPLIT(@OriginatorKeyColumns, ';')) m ON c.COLUMN_NAME = m.COLUMN_NAME
WHERE 
	TABLE_NAME = @TableNameR AND TABLE_SCHEMA = @TableSchemaR
-- PRINT 'OnStatement = ' + isnull(@OnStatement, '')

SELECT @SetStatement =
	STRING_AGG(QUOTENAME(COLUMN_NAME) + ' = b.' + QUOTENAME(COLUMN_NAME), ',') 
FROM (
	SELECT 
		c.COLUMN_NAME
	FROM INFORMATION_SCHEMA.COLUMNS c INNER JOIN 
		(select value COLUMN_NAME from STRING_SPLIT(@MatchedColumns, ';')) m ON c.COLUMN_NAME = m.COLUMN_NAME
	WHERE 
		TABLE_NAME = @TableNameR AND TABLE_SCHEMA = @TableSchemaR
	EXCEPT
	SELECT 
		value 
	FROM 
		STRING_SPLIT(@OriginatorKeyColumns, ';')
) a
-- PRINT 'SetStatement' + @SetStatement

SELECT @UpdateCommand = @NL +
	'WHILE @PageNumber <= @PageCount' + @NL +
	'BEGIN' + @NL +
	'UPDATE ' + @table_name + ' ' + @NL +
	'SET ' + @setstatement + @NL +
	'FROM ' + @table_name + ' a' + @NL +
	'INNER JOIN (' + @NL +
	'SELECT TOP (@pagesize) ' + @ColumnList + @NL +
	'FROM ' + @log_table_name + @NL +
	'WHERE SYS_CHANGE_OPERATION = ' + '''' + 'U' + '''' + @NL +
	'and (@PageNumber - 1) * @PageSize < RowNum' + @NL +
	') b ON ' + @NL +
	@onstatement + @NL + @NL +
	'SET @PageNumber +=1'+ @NL +
	'END' 	
-- PRINT 'UpdateCommand = ' + @UpdateCommand


SELECT @DeleteCommand = @NL +
	'WHILE @PageNumber <= @PageCount' + @NL +
	'BEGIN' + @NL +
	'DELETE ' + @table_name + ' ' + @NL +
	'FROM '+ @table_name + ' a INNER JOIN ' + @log_table_name + ' b ON ' + @NL +
	@onstatement + @NL+
	'WHERE b.SYS_CHANGE_OPERATION = ' + '''' + 'D' + '''' + @NL +
	'and (@PageNumber - 1) * @PageSize < b.RowNum' + @NL + @NL +
	'SET @PageNumber +=1'+ @NL +
	'END' 	
--PRINT 'DeleteCommand = ' + @DeleteCommand

SELECT @SQL =
	'CREATE PROCEDURE ' + @spName + ' ' +
	'AS' + @NL +
	'BEGIN' + @NL +
	'BEGIN TRANSACTION; ' + @NL + 
	'BEGIN TRY' + @NL + 
	'SET NOCOUNT ON' + @NL + @NL +
	'DECLARE @PageSize bigint = ' + convert(nvarchar(50), @PageSize) + ', ' + '@PageNumber bigint' + ', ' + '@TotalRows bigint' + ', ' + '@PageCount bigint' + @NL + @NL + 
	'SET @TotalRows = (SELECT count(*) from ' + @log_table_name + ' WHERE SYS_CHANGE_OPERATION = ' + '''' + 'I' + ''''  + ')' + @NL +
	'SET @PageCount = @TotalRows / @PageSize + 1' + @NL +
	'SET @PageNumber = 1' + @NL +
	@InsertCommand + @NL + @NL

	IF @DoUpdateCommand = 1
	BEGIN
		SET @SQL = @SQL +
		'SET @TotalRows = (SELECT count(*) from ' + @log_table_name + ' WHERE SYS_CHANGE_OPERATION = ' + '''' + 'U' + ''''  + ')' + @NL +
		'SET @PageCount = @TotalRows / @PageSize + 1' + @NL +
		'SET @PageNumber = 1' + @NL +
		@UpdateCommand + @NL + @NL
	END

SELECT @SQL = @SQL +
	'SET @TotalRows = (SELECT count(*) from ' + @log_table_name + ' WHERE SYS_CHANGE_OPERATION = ' + '''' + 'D' + ''''  + ')' + @NL +
	'SET @PageCount = @TotalRows / @PageSize + 1' + @NL +
	'SET @PageNumber = 1' + @NL +
	@DeleteCommand + @NL +
	'END TRY' + @NL + 
	'BEGIN CATCH ' + @NL + 
	'IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;' + @NL + 
	'END CATCH; ' + @NL + 
	'IF @@TRANCOUNT > 0 COMMIT TRANSACTION;' + @NL +

	'END '

--print 'SQL = ' + @SQL
EXEC sys.sp_executesql @SQL

END

END