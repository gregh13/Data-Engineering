# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2
import ibm_db
# Connect to MySQL
connection = mysql.connector.connect(user='root', password='MTcxMjYtZ3JlZ2hl',host='127.0.0.1',database='sales')
cursor = connection.cursor()

# Connect to DB2
dsn_hostname = "b1bc1829-6f45-4cd4-bef4-10cf081900bf.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = "hft39186"        # e.g. "abc12345"
dsn_pwd = "Ph2bbAyqbxZjTFtH"      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = "32304"                # e.g. "50000" 
dsn_database = "bludb"            # i.e. "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}" # i.e. "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              # i.e. "SSL"

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

conn = ibm_db.connect(dsn, "", "")
print("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

# Find out the last rowid from DB2 data warehouse

# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database.

def get_last_rowid():
    SQL = "SELECT rowid FROM sales ORDER BY rowid DESC LIMIT 1"
    stmt = ibm_db.exec_immediate(conn, SQL)
    rowid = ibm_db.fetch_tuple(stmt)
    return rowid[0]


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    SQL = f"SELECT * FROM sales_data WHERE rowid > {rowid}"	
    cursor.execute(SQL)
    row_list = cursor.fetchall()
    return row_list

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database.

def insert_records(records):
    SQL = "INSERT INTO sales(rowid,product_id,customer_id,quantity) VALUES(?,?,?,?)"
    stmt = ibm_db.prepare(conn, SQL)
    for row in records:
        ibm_db.execute(stmt, row)
	

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from DB2 data warehouse
ibm_db.close(conn)
# End of program
