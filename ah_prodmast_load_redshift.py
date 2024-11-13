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
python3 ah_prodmast_load_redshift.py -c <config_file> -d <run_dt>
example :
python3 ah_prodmast_load_redshift.py -c ah_redshift.config -d 20210517
"""

def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        opts, args = getopt.getopt(argv, "hd:c:", ["date=", "config="])
    except getopt.GetoptError:
        print('ah_prodmast_load_redshift.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('ah_prodmast_load_redshift.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : ah_prodmast_load_redshift.py -c <config_file> -d <rundate>')
                sys.exit()

    print('config file is :', config_file)
    print('run date is :', run_dt)

    listOfGlobals = globals()
    listOfGlobals['run_date'] = run_dt

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

    ex_sql_file(params)

    print('state : complete')    


def ex_sql_file(params):
    #print('inside ex_sql_file()')

    if params['S3PATH']:
        s3file = params['S3PATH']
    print("\n s3file is : ", s3file)

    if params['DBNAME'] and params['HOST'] and params['PORT'] and params['USER'] and params['PASSWORD']:
        print('all well')
                
        #pm_copy_command = "copy prod.ahnext_prodmast from '" + s3file + "/ahnext_prodmast/" + run_date + "/" + f"mod_prodMast_{run_date}.csv.gz" "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '|' escape timeformat 'YYYY-MM-DD HH:MI:SS' REMOVEQUOTES IGNOREHEADER 2 NULL AS 'NULL' ACCEPTINVCHARS AS ' ' maxerror as 2;"
        
        pm_copy_command = "copy prod.ahnext_prodmast from '" + s3file + "/ahnext_prodmast/" + run_date + "/" + f"mod_prodMast_{run_date}.csv.gz" "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '|' escape timeformat 'YYYY-MM-DD HH:MI:SS' REMOVEQUOTES IGNOREHEADER 2 NULL AS 'NULL' ACCEPTINVCHARS AS ' ' maxerror as 2;"
        print("\n copy command for ProdMast table is :", pm_copy_command)
        
        conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
        cur = conn.cursor();

  
        cur.execute("begin;")
        cur.execute("truncate table prod.ahnext_prodmast;")
        cur.execute(pm_copy_command)
        cur.execute("commit;")
        
        pm_count_sql = 'select count(*) from prod.ahnext_prodmast'
        cur.execute(pm_count_sql)
        pm_count_tup = cur.fetchall()[0]
        pm_count = ''.join(map(str,pm_count_tup))
        pm_msg = pm_count + ' Records loaded in prod.ahnext_prodmast table'
        print(pm_msg)
        
        conn.close()
        print("Copy commands for ProdMast tables executed fine!")


if __name__ == "__main__":
    main(sys.argv[1:])
