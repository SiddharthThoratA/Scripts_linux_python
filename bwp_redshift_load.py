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
python3 bwp_redshift_load.py -c <config_file> -t <tbl_name>
example :
python3 bwp_redshift_load.py -c bwd_redshift.config -t big_wip_data_with_size
"""

def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        opts, args = getopt.getopt(argv, "c:t:", ["config=", "tbl_name="])
    except getopt.GetoptError:
        print('bwp_redshift_load.py -c <config_file> -t <tbl_name>')
    for opt, arg in opts:
        if opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : bwp_redshift_load.py -c <config_file> -t <tbl_name>')
                sys.exit()
        elif opt in ('-t', '--tbl_name'):
            tbl_name = arg
            if not tbl_name:
                print('Missing config file name --> syntax is : bwp_redshift_load.py -c <config_file> -t <tbl_name>')
                sys.exit()

    print('config file is :', config_file)
    print('table name is :', tbl_name)

    #print("Done!")
    read_config(config_file,tbl_name)

def read_config(config_file,tbl_name):
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
    #print(params)

    ex_sql_file(params,tbl_name)

    print('state : complete')

def ex_sql_file(params,tbl_name):

    if params['DBNAME'] and params['HOST'] and params['PORT'] and params['USER'] and params['PASSWORD']:
        print('all well')
        sql_command = "call jesta_prod_ft.sp_load_big_wip_data_with_size();"
        conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
        cur = conn.cursor();
        # Begin your transaction
        cur.execute("begin;")
        cur.execute(sql_command)
        cur.execute("commit;")
        count_sql = 'select count(*) from jesta_prod_ft.' + tbl_name
        cur.execute(count_sql)
        conn.commit()
        count_tup = cur.fetchall()[0]
        count = ''.join(map(str,count_tup))
        msg = count + ' : Records inserted in table ' + tbl_name
        print(msg)
        conn.close()

if __name__ == "__main__":
    main(sys.argv[1:])


