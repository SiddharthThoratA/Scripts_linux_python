import sys, getopt
import datetime
import csv
import gzip
import os
import psycopg2


"""
how to call this script :
python3 ah_load_redshift.py -d ${run_dt} -f ddl.sql -c ah_redshift.config -m table_name
example :
python3 ah_load_redshift.py -d 20240318 -c ah_redshift.config -e /home/dixit/Ah_ext/sql/ddl_dd.sql -m demand_details
"""

run_dt=''

def main(argv):

    try:
        opts, args = getopt.getopt(argv, "hd:c:e:m:", ["date=", "config=", "ddl_lst=", "main_tbl="])
    except getopt.GetoptError:
        print('ah_load_redshift.py -d <rundate> -c <config_file> -e <ddl_sql_file> -m <table_name>')
    for opt, arg in opts:
        if opt == '-h':
            print('ah_load_redshift.py -d <rundate> -c <config_file> -e <ddl_sql_file> -m <table_name>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
        elif opt in ('-c', '--config'):
            config_file = arg
        elif opt in ('-e', '--ddl_lst'):
            ddl_lst = arg
        elif opt in ('-m', '--main_tbl'):
            main_tbl = arg
            if not config_file:
                print('Missing config file name --> syntax is : ah_load_redshift.py -d <rundate> -c <config_file> -e <ddl_sql_file> -m <table_name>')
                sys.exit()

    print('config file is :', config_file)
    print('run date is :', run_dt)
    print('DDL List file is :', ddl_lst)
    print('Main Table  is :', main_tbl)

    listOfGlobals = globals()
    listOfGlobals['run_date'] = run_dt
    listOfGlobals['main_tbl'] = main_tbl

    read_config(config_file,ddl_lst)

def read_config(config_file,ddl_lst):
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

    ex_list_file(params,ddl_lst)
    
    print('state : complete')

def ex_list_file(params,ddl_lst):
    
    if params['S3PATH']:
        s3file = params['S3PATH']
    print("\n s3file is : ", s3file)
    
    temp_tbl = main_tbl+'_temp'
    if params['DBNAME'] and params['HOST'] and params['PORT'] and params['USER'] and params['PASSWORD']:
        print("Redshift connection established")
        
        Total_count = "select count(*) from prod."+main_tbl
        #print("\n Total count of table is :", Total_count)
        
        conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
        ddl_file=ddl_lst
        with open(ddl_file,'r') as ddl_file:
            create_table = ddl_file.read()
            
        
        cur = conn.cursor();
        
        #Drop temp table if exists
        cur.execute("begin;")
        cur.execute("drop table if exists prod."+temp_tbl)
        print('Temp table dropped')
        cur.execute("commit;")
        
        #Create temp table
        cur.execute("begin;")
        cur.execute(create_table)
        print('Temp table created')
        cur.execute("commit;")
        
        if main_tbl == 'demand_details':
            copy_command = "copy prod.demand_details_temp from '" + s3file + "/Demand_Details/" + run_date + "/" + "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '~' timeformat 'YYYY-MM-DD HH:MI:SS' IGNOREHEADER 2 NULL AS 'NULL' ACCEPTINVCHARS AS ' ' maxerror as 250;"
            print("\n copy command for Demand_Details table is :", copy_command)
        elif main_tbl == 'grossmargin_details':
            copy_command="copy prod.grossmargin_details_temp from '" + s3file + "/GrossMargin_Details/" + run_date + "/" + "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '|' escape timeformat 'YYYY-MM-DD HH:MI:SS' REMOVEQUOTES IGNOREHEADER 2 NULL AS 'NULL' ACCEPTINVCHARS AS ' ' maxerror as 250;"
            print("\n copy command for grossmargin_details table is :", copy_command)
        elif main_tbl == 'returns_details':
            copy_command="copy prod.returns_details_temp from '" + s3file + "/Return_Details/" + run_date + "/" + "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '|' escape timeformat 'YYYY-MM-DD HH:MI:SS' REMOVEQUOTES IGNOREHEADER 2 NULL AS 'NULL' ACCEPTINVCHARS AS ' ' maxerror as 250;"
            print("\n copy command for Return_Details table is :", copy_command)
        elif main_tbl == 'ahnext_dropshippos':
            copy_command="copy prod.ahnext_dropshippos_temp from '" + s3file + "/DropShipPOs/" + run_date + "/" + "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '|' escape timeformat 'YYYY-MM-DD HH:MI:SS' REMOVEQUOTES IGNOREHEADER 2 NULL AS 'NULL' ACCEPTINVCHARS AS ' ' maxerror as 250;"
            print("\n copy command for DropShipPOs table is :", copy_command)
        elif main_tbl == 'loyaltymembers':
            copy_command="copy prod.loyaltymembers_temp from '" + s3file + "/LoyaltyMembers/" + run_date + "/" + "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' gzip delimiter '|' escape timeformat 'YYYY-MM-DD HH:MI:SS' REMOVEQUOTES IGNOREHEADER 2 NULL AS 'NULL' ACCEPTINVCHARS AS ' ' maxerror as 250;"
            print("\n copy command for loyaltymembers table is :", copy_command)
        
        
        #exceute copy command
        cur.execute("begin;")
        cur.execute(copy_command)
        cur.execute("commit;")
        
        
        #Drop base table and rename temp table name to base table
        cur.execute("begin;")
        cur.execute("drop table if exists prod."+main_tbl)
        print("Main table dropped")
        cur.execute("alter table prod."+ temp_tbl +" rename to "+ main_tbl)        
        print("Temp table name changed")
        cur.execute("commit;") 
              
        cur.execute("begin;")
        cur.execute(Total_count)
        count_tup = cur.fetchall()[0]
        count = ''.join(map(str,count_tup))
        msg = count + ' Records loaded in prod.'+ main_tbl +' table'
        print(msg)
        cur.execute("commit;")
        
        conn.close()
        
        print("Copy executed fine!")
 

if __name__ == "__main__":
    main(sys.argv[1:])


