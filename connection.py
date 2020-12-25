import cx_Oracle
import pymysql

no_of_records=30
start_date = '1/1/2020 1:30 PM'
end_date = '12/1/2020 4:50 AM'

MYSQL_SITES=['S4','S3']

dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XE")
conn = cx_Oracle.connect("SYS", "SYS", dsn, cx_Oracle.SYSDBA, encoding="UTF-8")
print(conn)

# Open MYSQL connection
mySqlCon = pymysql.connect("localhost","devgamingstaan","@Hani8655","GamingStaan" )

print(mySqlCon)
# make_query()
