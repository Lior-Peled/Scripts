---PER DB---


select    
      sum(reserved_page_count) * 8.0 / 1024 [SizeInMB]
from    
      sys.dm_db_partition_stats




---PER TABLE--

	  select  schema_name(tab.schema_id) + '.' + tab.name as [table], 
    cast(sum(spc.used_pages * 8)/1024.00 as numeric(36, 2)) as used_mb,
    cast(sum(spc.total_pages * 8)/1024.00 as numeric(36, 2)) as allocated_mb,
	MAX(rc.[RowCount])AS  [RowCount]
from sys.tables tab
join sys.indexes ind 
     on tab.object_id = ind.object_id
join sys.partitions part 
     on ind.object_id = part.object_id and ind.index_id = part.index_id
join sys.allocation_units spc
     on part.partition_id = spc.container_id
-- row count-- 
left join
(
----row count---
SELECT
              QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [QuoteTableName],
	     SCHEMA_NAME(sOBJ.schema_id) + '.' + sOBJ.name AS [TableName],
       SUM(sPTN.Rows) AS [RowCount]
FROM 
      sys.objects AS sOBJ
      INNER JOIN sys.partitions AS sPTN
            ON sOBJ.object_id = sPTN.object_id
WHERE
      sOBJ.type = 'U'
      AND sOBJ.is_ms_shipped = 0x0
      AND index_id < 2 -- 0:Heap, 1:Clustered
	  
GROUP BY 
      sOBJ.schema_id
      , sOBJ.name  ) rc    on    rc.TableName =  schema_name(tab.schema_id) + '.' + tab.name
group by schema_name(tab.schema_id) + '.' + tab.name, rc.TableName
order by sum(spc.used_pages) desc;

