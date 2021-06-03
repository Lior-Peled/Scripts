

 Declare @original nvarchar(100) = '',
         @new  nvarchar(100) = '',
		 @sql nvarchar(max) =N'',
		 @counter int = 1,
		 @table_name nvarchar(100) = 'mrr_company',
		 @ColumnsToIterate int = 0

	set @ColumnsToIterate = (	 select count(1) from 
		                         INFORMATION_SCHEMA.COLUMNS
                                  where TABLE_NAME = @table_name)



		 while @counter <=  @ColumnsToIterate
		 BEGIN
		 SELECT @original= REPLACE(REPLACE(oRIginal,'[',''),']','') , @new= REPLACE(REPLACE(new,'[',''),']','')
		 FROM 
		(  select quotename(COLUMN_NAME) as oRIginal , replace(quotename(COLUMN_NAME), '[ ', '[') as new, 
        row_number() over (order by (select (null))) as rn from INFORMATION_SCHEMA.COLUMNS
 where TABLE_NAME = @table_name ) A
          WHERE RN = @counter

		
		 set @Sql = N'  EXEC sp_rename '''+@table_name+'.'+@original+''', '''+@NEW+''' , ''COLUMN'''
		PRINT @SQL 
		EXECUTE (@sQL)
		SET @COUNTER= @COUNTER +1


		END
