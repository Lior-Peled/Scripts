import pyodbc

class SqlServer:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password

    def connect(self):
        self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password)

    def query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def insert_data(self, table, columns, values):
        query = 'INSERT INTO ' + table + ' (' + columns + ') VALUES (' + values + ')'
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()

    def delete(self, table, condition):
        query = 'DELETE FROM ' + table + ' WHERE ' + condition
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()

        
#To use this class, you would first create an instance of the SqlServer class, 
#passing in the server, database, username, and password as arguments to the _init_ method. 
#Then, you can use the connect method to connect to the SQL Server database, 
#the query method to execute a query, the insert_data method to insert data into a table,
#the delete method to delete data from a table, and the close method to close the connection to the database. 

#EXAMPLES:
# Create an instance of the SqlServer class.
sql_server = SqlServer('my-server', 'my-database', 'my-username', 'my-password')

# Connect to the SQL Server database.
sql_server.connect()

# Execute a query to get some data from the database.
rows = sql_server.query('SELECT * FROM my_table')

# Insert some data into a table in the database.
sql_server.insert_data('my_table', 'column1, column2, column3', 'value1, value2, value3')

# Delete rows from a table in the database.
sql_server.delete('my_table', 'column1 = value1')

# Close the connection to the database.
sql_server.close()