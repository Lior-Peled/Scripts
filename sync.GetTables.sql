/****** Object:  StoredProcedure [sync].[GetTables]    Script Date: 7/13/2021 6:01:19 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [sync].[GetTables] @ID [int],@RunID [uniqueidentifier] AS
BEGIN

SET NOCOUNT ON

-- sample execution
-- exec [sync].[GetTables] -1, 'c356cbfd-4f41-42a0-899f-7589dd8b3e71'

SELECT 
	a.[ID]
	,a.[TableSchemaSource] TableSchemaS
	,a.[TableNameSource] TableNameS
	, QUOTENAME( a.[TableSchemaSource] ) + '.' + QUOTENAME( a.[TableNameSource] ) TableSource

	,a.[TableSchemaRecipient] TableSchemaR
	,a.[TableNameRecipient] TableNameR
	, QUOTENAME( a.[TableSchemaRecipient] ) + '.' + QUOTENAME( a.[TableNameRecipient] ) TableRecipient

	,isnull(b.[LastVersion], -1) [LastVersion]
	,isnull(b.[LastSyncRowsCount], -1) [LastSyncRowsCount]
	,b.MatchedColumns
	,b.SourceKeyColumns
	,c.mapping
	,d.mappingincremental
	,e.mappingkey

	,'sync' TableLogSchema
	, 'Log_' + a.[TableNameRecipient] + replace(@RunID, '-', '')  TableLogName
	, QUOTENAME('sync') + '.' + QUOTENAME('Log_' + a.[TableNameRecipient] + replace(@RunID, '-', '') ) TableLog

	, 'dbo' TableKeySchema
	, 'Key_' + a.[TableNameSource] + replace(@RunID, '-', '') TableKeyName
	, QUOTENAME('dbo') + '.' + QUOTENAME( 'Key_' + a.[TableNameSource] + replace(@RunID, '-', '')) TableKey
  FROM 
	[sync].[Management] a LEFT OUTER JOIN
	[sync].[ManagementInfo] b ON a.ID = b.ID

OUTER APPLY 
(
	SELECT 
		'{ "type":"TabularTranslator", "mappings": [' + 
		string_agg(cast('{"source":{"name":"' + value + '"},"sink":{"name":"' + value + '"}}' as NVARCHAR(MAX)), ', ') +
		']}' mapping
	FROM string_split(b.[MatchedColumns], ';')
) c

OUTER APPLY 
(
	SELECT 
		'{ "type":"TabularTranslator", "mappings": [' + 
		string_agg(cast('{"source":{"name":"' + value + '"},"sink":{"name":"' + value + '"}}' as NVARCHAR(MAX)), ', ') +
		']}' mappingincremental
	FROM string_split(b.[MatchedColumns] + ';SYS_CHANGE_VERSION;SYS_CHANGE_OPERATION;RowNum', ';')
) d

OUTER APPLY 
(
	SELECT 
		'{ "type":"TabularTranslator", "mappings": [' + 
		string_agg(cast('{"source":{"name":"' + value + '"},"sink":{"name":"' + value + '"}}' as NVARCHAR(MAX)), ', ') +
		']}' mappingkey
	FROM string_split(b.[SourceKeyColumns], ';')
) e

	WHERE 1 = CASE
              WHEN @ID = -1 THEN 1
              ELSE 0 
           END OR 
			  a.[ID] = @ID
END




--example of mapping column values:

{ "type":"TabularTranslator", "mappings": [{"source":{"name":"IDInvoicedReportFileDetailsApproval"},
"sink":{"name":"IDInvoicedReportFileDetailsApproval"}},
{"source":{"name":"IDInvoicedReportFile"},
"sink":{"name":"IDInvoicedReportFile"}}, 
{"source":{"name":"DateOfDispense"},
"sink":{"name":"DateOfDispense"}}, 
{"source":{"name":"PrescriptionNo"},
"sink":{"name":"PrescriptionNo"}}, 
{"source":{"name":"PercentageReplenished"},
"sink":{"name":"PercentageReplenished"}},
{"source":{"name":"TotalPayableAmount"},
"sink":{"name":"TotalPayableAmount"}}, 
{"source":{"name":"InvoiceNumber"},
"sink":{"name":"InvoiceNumber"}},
{"source":{"name":"DateOfPurchase"},"sink":{"name":"DateOfPurchase"}}]}



--example of mapping key:
{ "type":"TabularTranslator", "mappings": [{"source":{"name":"IDAccountType"},
"sink":{"name":"IDAccountType"}}]}