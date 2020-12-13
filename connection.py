import cx_Oracle

no_of_records=50
start_date = '1/1/2020 1:30 PM'
end_date = '12/1/2020 4:50 AM'

dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XE")
conn = cx_Oracle.connect("SYS", "SYS", dsn, cx_Oracle.SYSDBA, encoding="UTF-8")
print(conn)

# make_query()
