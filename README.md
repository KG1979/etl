Requirement 8: upload Hbase table by reading configuration from new table pmc_map_table.
1.	Create New table pmc_map_table in mysql
2.	New table has following columns:
a.	FID --- this ID generated automatically 
b.	HBaseTable – this is the HBastTable name. Exa: test1, sample etc
c.	Delimiter – this is the data file delimiter. Include all possible delimiter in a file like comma, tab etc
d.	ColumnFamily  -- this is the ColumnFamily to be used in Hbase table. Ex: pt is the column family name
e.	RowKeyColumnIndex  -- this is the columnIndex matching to column number in data file. Ex: If data file has four columns – FirstName, LastName, RowKey, Address. If we give ColumnIndex = 3, then program should use RowKey has the rowkey in HbaseTable upload
f.	DataColumnIndex – this is the DataColumnIndex matching to column number in data file. Ex: If data file has four columns – FirstName, LastName, RowKey, Address. If we give ColumnIndex = 2, then program should refer second column in data file ie., LAST NAME and upload only LAST NAME as column in hbase table
g.	Column – this is column name in Hbase table. Ex: If we provide ‘UsedName’ as the column, program will upload DataColumnIndex values into this column. In this case ‘LastName’ in data file will be uploaded as ‘UsedName’ in Hbase table
3.	When running the program following parameters must be sent:
a.	FID, HBaseTable Name, Control file name, QueName
4.	See if you can use existing ETL project by creating NEW VERSION only used for this requirement
Example:
a.	Consider we have created hbase table as follows: HbaseTable Name = test, columnfamily = pt
b.	Data file has four columns – FirstName, LastName, RowKey, Address. This file is comma delimited
Sample data file:
FirstName, LastName, RowKey, Address
Krish,gowda,123,berry chase way
Partiv,patel,234,niagra street
Prashant,kuntu,3456788,cobra avenue
c.	In pmc_map_table we provided below configuration:
a.	HBaseTable = ‘test’
b.	Delimiter = ‘,’
c.	ColumnFamily = ‘pt’
d.	RowKeyColumnIndex = 3
e.	DataColumnIndex = 2
f.	Column=’UsedName’
d.	After running the program Hbase table = test should have following values	
 	123	column=pt:UsedName,timestamp =1421865543911,value=gowd
	234	column=pt:UsedName,timestamp =1421865543911,value=patel
	3456788	column=pt:UsedName,timestamp =1421865543911,value=kunt
