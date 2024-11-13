import sys, getopt
import datetime
import boto3
import csv
import gzip
import os
import psycopg2
import pandas as pd
import logging


#run_date = datetime.datetime.now().strftime("%Y%m%d")
#run_date = ''

"""
how to call this script :
python3 wmpi_redshift_load.py -c <config_file> -d <run_dt>
example :
python3 wmpi_redshift_load.py -c wmpi_transaction_redshift.config -d 20221003
"""

def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        opts, args = getopt.getopt(argv, "hd:c:", ["date=", "config="])
    except getopt.GetoptError:
        print('wmpi_redshift_load.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('wmpi_redshift_load.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : wmpi_redshift_load.py -c <config_file> -d <rundate>')
                sys.exit()

    print('config file is :', config_file)
    #print('Previous date is :', run_dt)

    listOfGlobals = globals()
    listOfGlobals['prev_date'] = run_dt
    
    print("************************************")
    print('Previous date is :',prev_date)
    print("************************************")
    
    logging.basicConfig(filename=f'/home/dstdw/Marketplace/Walmart_transaction/log_files/column_mismatched_file_{prev_date}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
  
    
    formatted_date = datetime.datetime.strptime(prev_date, "%Y%m%d").strftime("%Y-%m-%d")
    print('Formated data is: ',formatted_date)
    print("************************************")
    
    listOfGlobals['formatted_date'] = formatted_date
    
    #print("Done!")
    read_config(config_file)
    
def read_config(config_file):
    conf_file = config_file
    fileobj = open(conf_file)
    params = {}
    for line in fileobj:
        line = line.strip()
        if not line.startswith('#'):
            conf_value = line.split('=')
            print('conf_value before: ',conf_value)
            if len(conf_value) == 2:
                params[conf_value[0].strip()] = conf_value[1].strip()
            print('conf_value after: ',conf_value)
    fileobj.close()

    #params.update(S3PATH = list_file)
    print(params)
    
    listOfGlobals = globals()
    listOfGlobals['params'] = params
    
    
    #call this function to load file from S3 to Redshift table
    ex_sql_file()
    print('state : complete')


def ex_sql_file():
    if params['S3PATH']:
        s3file = params['S3PATH']
    print("\n s3file is : ", s3file)

    if params['DBNAME'] and params['HOST'] and params['PORT'] and params['USER'] and params['PASSWORD']:
        print('all well')
        wal_mpi_transaction_copy_cmd = "copy marketplace_prod.walmart_transactions from '" + s3file + "/" + prev_date + "/" + "' WITH CREDENTIALS AS 'aws_iam_role=arn:aws:iam::350027143148:role/dst-redshift-access' DATEFORMAT AS 'auto' TIMEFORMAT AS 'auto' IGNOREHEADER AS 1 delimiter '|' REMOVEQUOTES escape;"
           
        print("\n copy command for Walmart MPI Transaction table is :", wal_mpi_transaction_copy_cmd)

        Total_table_count = "select count(*) from marketplace_prod.walmart_transactions;"
        print("\n Total count of table is :", Total_table_count)
        
        daily_load_count = f"select count(*) from marketplace_prod.walmart_transactions where transaction_date = '{prev_date}';"
        print("\n Today's file count is :", daily_load_count)

        conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
        cur = conn.cursor();

        cur.execute("begin;")
        cur.execute(wal_mpi_transaction_copy_cmd)
        cur.execute("commit;")
        
        cur.execute("begin;")
        cur.execute(Total_table_count)
        count_tup = cur.fetchall()[0]
        count = ''.join(map(str,count_tup))
        msg = count + ' total Records count'
        print(msg)
        cur.execute("commit;")
        
        cur.execute("begin;")
        cur.execute(daily_load_count)
        file_count = cur.fetchall()[0]
        today_count = ''.join(map(str,file_count))
        total = today_count + ' Records loaded in walmart_mpi_transaction table'
        print(total)
        cur.execute("commit;")
        
        conn.close()
        print("Copy commands for walmart_mpi_transaction table executed fine!")

if __name__ == "__main__":
    main(sys.argv[1:])
