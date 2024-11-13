import sys, getopt
import datetime
import csv
import gzip
import os
import psycopg2

#run_date = datetime.datetime.now().strftime("%Y%m%d")
run_date = ''

"""
how to call this script :
python3 image_availability_s3_to_rs_load.py -d <rundate> -f <copy_sql_list_file> -c <config_file>
example :
python3 image_availability_s3_to_rs_load.py -d 20210629 -f rs_copy.lst -c redshift_conn.config
"""

def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #opts = []
    try:
        opts, args = getopt.getopt(argv, "hd:f:c:", ["date=", "cp_list=", "config="])
    except getopt.GetoptError:
        print('image_availability_s3_to_rs_load.py -d <rundate> -f <copy_sql_list_file> -c <config_file>')
    for opt, arg in opts:
        if opt == '-h':
            print('image_availability_s3_to_rs_load.py -d <rundate> -f <copy_sql_list_file> -c <config_file>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
        elif opt in ('-f', '--cp_list'):
            cp_file = arg
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : image_availability_s3_to_rs_load.py -d <rundate> -f <copy_sql_list_file> -c <config_file>')
                sys.exit()

    print('Copy List file is :',cp_file)
    print('config file is :', config_file)
    print('run date is :', run_dt)

    listOfGlobals = globals()
    listOfGlobals['run_date'] = run_dt

    #print("Done!")
    read_config(config_file,cp_file)

def read_config(config_file,cp_file):
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

    #params.update(S3PATH = list_file)
    print(params)

    ex_list_file(params,cp_file)


def ex_list_file(params,cp_file):
    #print('inside ex_sql_file()')
    cp_list = cp_file
    if params['S3PATH']:
        s3file = params['S3PATH'] + '/' + run_date
    print("\n s3file is : ", s3file)

    if params['DBNAME'] and params['HOST'] and params['PORT'] and params['USER'] and params['PASSWORD']:
        #print('all well')
        #copy_command = "copy jesta_stage_ft.data_availability_whs from '" + s3file + "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '|' escape timeformat 'YYYY-MM-DD HH:MI:SS';"
        #print("\n copy command is :", copy_command)
        conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
        cur = conn.cursor();
        cp_fileobj = open(cp_list)
        for cp_line in cp_fileobj:
            #copysql = cp_line.split(";")[0].replace('RUN_DATE',run_date)
            copysql = cp_line.replace('RUN_DATE',run_date)
            print("\n SQL post RUN_DATE replaced : ", copysql)
            # Begin your transaction
            cur.execute("begin;")
            cur.execute(copysql)
            # Commit your transaction
            cur.execute("commit;")
        cp_fileobj.close()
        conn.close()
        print("Copy executed fine!")


if __name__ == "__main__":
    main(sys.argv[1:])
