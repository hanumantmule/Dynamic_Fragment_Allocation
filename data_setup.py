import csv
import string
from connection import *
import random
from datetime import datetime
import time


def str_time_prop(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)


def insert_log_info(fragment_no, site_no, adatetime, access_type, data_vol, table_site):
    # construct an insert statement that add a new row to the billing_headers table
    table_name = 'log_info_' + table_site.lower()
    sql = ('insert into ' + table_name + ' (AFID, ASID, ADateTime, RorWAS, DataVol) '
                                         'values(:fragment_no,:site_no,:adatetime, :access_type, :data_vol)')

    mysql_query = ('insert into ' + table_name + ' (AFID, ASID, ADateTime, RorWAS, DataVol) values (%s,%s,%s,%s,%s)')

    if table_site in MYSQL_SITES:
        cursor = mySqlCon.cursor()
        cursor.execute(mysql_query, [fragment_no, site_no, adatetime, access_type, data_vol])
        cursor.close()
        # commit work
        mySqlCon.commit()
    else:
        cursor = conn.cursor()
        cursor.execute(sql, [fragment_no, site_no, adatetime, access_type, data_vol])
        # commit work
        conn.commit()


def insert_log_data(fragment_list, site_no, sites):
    RorW = ['r', 'w']
    print("Generating Query Access Log for each site:\n")
    for i in range(1, no_of_records):
        random_fragment = random.choice(fragment_list)
        random_site = random.choice(sites)
        data_vol = random.randint(100, 700)
        adatetime = random_date(start_date, end_date, random.random())
        access_type = random.choice(RorW)
        print(random_fragment, random_site, data_vol, adatetime, access_type, site_no)
        insert_log_info(random_fragment, random_site, adatetime, access_type, data_vol, site_no)


def insert_employee():
    cur = conn.cursor()
    with open("emp_data.csv", "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for lines in csv_reader:
            cur.execute("insert into Employee (Employee_ID, First_Name, Last_Name, Gender, Email,"
                        "Date_of_birth, Date_of_joining, Salary, SSN, Phone_number, Place_Name, County,City) values "
                        "(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)",
                        (lines[0], lines[1], lines[2], lines[3],
                         lines[4], lines[5], lines[6], lines[7], lines[8], lines[9], lines[10], lines[11], lines[12]))

    cur.close()
    conn.commit()


def fetch_distinct_sites():
    list_static_fragment_query = 'select distinct(site_name) from fragment_alloc_site_mapping'
    cur = conn.cursor()
    cur.execute(list_static_fragment_query)
    res = cur.fetchall()
    list_res = []
    for val in res:
        list_res.append(val[0])
    cur.close()
    return list_res


def fetch_access_threeshold_per_site(dist_site_name):
    query = 'select distinct(access_threeshold) from fragment_alloc_site_mapping where site_name= :dist_site_name'
    cur = conn.cursor()
    cur.execute(query, [dist_site_name])
    cur.execute(query)
    res = cur.fetchall()
    for val in res:
        acc_threeshold = val[0]
        break
    cur.close()
    return acc_threeshold or 0


def fetch_distinct_fragments(fragment_query, dist_site_name):
    cur = conn.cursor()
    cur.execute(fragment_query, [dist_site_name])
    res = cur.fetchall()
    list_res = []
    for val in res:
        list_res.append(val[0])
    cur.close()
    return list_res


def fetch_static_fragment_alloc():
    dictionary = {}
    list_distinct_sites = fetch_distinct_sites()
    print("All sites: " + str(list_distinct_sites))
    for site in list_distinct_sites:
        list_distinct_fragments_query = ('select distinct(fragment_name) from fragment_alloc_site_mapping where '
                                         'site_name= :site ')
        list_fragment_per_site = fetch_distinct_fragments(list_distinct_fragments_query, site)
        dictionary[site] = list_fragment_per_site
    return dictionary


def initial_setup(dictonary):
    for fragment, site in dictonary.items():
        realloc_update_query = 'update fragment_alloc_site_mapping set site_name= :site where fragment_name= :fragment'
        cur = conn.cursor()
        cur.execute(realloc_update_query, [site, fragment])
        res = conn.commit()
        cur.close()

    for i in range(1, 5):
        table_name = 'log_info_s' + str(i)
        realloc_update_query = 'truncate table ' + table_name

        if 'S' + str(i) in MYSQL_SITES:
            cur = mySqlCon.cursor()
        else:
            cur = conn.cursor()

        cur.execute(realloc_update_query)
        res = conn.commit()
        cur.close()
