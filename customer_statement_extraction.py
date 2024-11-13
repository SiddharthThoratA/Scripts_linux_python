import sys, getopt
import datetime
import csv
import gzip
import os
import psycopg2

"""
how to call this script :
python3 open_orders_minimum_hold_load_redshift.py -c <config_file> -f <sql_file_list>
example :
python3 open_orders_minimum_hold_load_redshift.py -c redshift_conn.config -f extraction_sql.lst
"""


def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        opts, args = getopt.getopt(argv, "hd:c:f:", ["date=", "config=", "file="])
    except getopt.GetoptError:
        print('open_orders_minimum_hold_load_redshift.py -c <config_file> -f <sql_file_list>')
    for opt, arg in opts:
        if opt == '-h':
            print('open_orders_minimum_hold_load_redshift.py -c <config_file> -f <sql_file_list>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : open_orders_minimum_hold_load_redshift.py -c <config_file> -f <sql_file_list>')
                sys.exit()
        elif opt in ('-f', '--file'):
            list_file = arg
            if not list_file:
                print('Missing config file name --> syntax is : open_orders_minimum_hold_load_redshift.py -c <config_file> -f <sql_list_file>')
                sys.exit()

    print('config file is :', config_file)
    print('sql list file is :', list_file)
    print('Rundate is :', run_dt)
    
    listOfGlobals = globals()
    listOfGlobals['run_date'] = run_dt
    #print("Done!")
    read_config(config_file,list_file)

def read_config(config_file,list_file):
    conf_file = config_file
    fileobj = open(conf_file)
    params = {}
    for line in fileobj:
        line = line.strip()
        if not line.startswith('#'):
            conf_value = line.split('=')
            if len(conf_value) == 2:
                params[conf_value[0].strip()] = conf_value[1].strip()
    fileobj.close()

    params.update(SQL_LIST_FILE = list_file)
    #params.update(S3PATH = list_file)
    print(params)

    ex_sql_file(params)

def ex_sql_file(params):
    print('inside ex_sql_file()')

    if params['SQL_LIST_FILE']:
        sql_list_file = params['SQL_LIST_FILE'].strip()
    else:
        print('There is not sql file to process --- exiting')
        sys.exit()
    print('start time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    ex_sql(sql_list_file,params)
    print('state : complete')
    print('completion time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def ex_sql(sql_list_file,params):
    if params['DBNAME'] and params['HOST'] and params['PORT'] and params['USER'] and params['PASSWORD']:
        print('all well')
        conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
    cur = conn.cursor();
    cur.execute("begin;")
    sql_list_read = open(sql_list_file)
    for line in sql_list_read:
        sql_file = line.strip()
        print('Executing sql file : ', sql_file)
        f = open(sql_file, "r")
        sql = f.read().split(";")[0]
        try:
            query_list_replaced = sql.replace('RUN_DATE', run_date)
            cur.execute(query_list_replaced)
        except Exception as e:
            print("Error msg : " + str(e))
            cur.close()
            conn.close()
            print('state : exception')
            exit()
    cur.execute("commit;")
    print('Execution of sql file complete')
    

if __name__ == "__main__":
    main(sys.argv[1:])
