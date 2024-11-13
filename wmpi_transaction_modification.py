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
python3 wmpi_transaction_modification.py -c <config_file> -d <run_dt>
example :
python3 wmpi_transaction_modification.py -c logility_redshift.config -d 20221003
"""

def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        opts, args = getopt.getopt(argv, "hd:t:c:", ["date=", "list=", "config="])
    except getopt.GetoptError:
        print('wmpi_inventory_modification.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('wmpi_inventory_modification.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-t','list='):
            list_file = arg
            print('List file is available')
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : wmpi_inventory_modification.py -c <config_file> -d <rundate>')
                sys.exit()

    print('config file is :', config_file)
    print('List file is :', list_file)
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
    
    
    #call this function to use values from the created dictionary and create directory in Unix
    ex_param_file(params)

    print('state : complete')


def ex_param_file(params):
    #print('inside ex_sql_file()')
        
    if params['OUT_FILE_LOC']:
        ofilepath = params['OUT_FILE_LOC'].strip()
        ofilepath = ofilepath.replace('RUN_DATE',prev_date)
        print("ofilepath is - ",ofilepath)
        
        #create a date folder in given path
        try:
            os.mkdir(ofilepath)
        except OSError:
            print ("Creation of the directory %s failed" % ofilepath)
        else:
            print ("Successfully created the directory %s " % ofilepath)
     
    #Calling read_list_file function
    read_list_file()

def modify_mpi_transaction(file_name):
    
    source_file_path=f'/home/dstdw/Marketplace/Walmart_transaction/data_files/{prev_date}'
    file_name = file_name
    
    wal_transaction_df = pd.read_csv(f'{source_file_path}/{file_name}',sep=',')
    
    expected_cols = ['Item_Name', 'SKU', 'GMV', 'Units_Sold', 'Orders', 'Offer_Pageviews','Offer_Conversion', 'Item_id', 'Base_Item_Id', 'Item_Pageviews','Item_Conversion', 'Auth_Sales', 'Cancelled_Sales', 'Refund_Sales','Department', 'Brand', 'Listing_Quality_Score', 'GMV_Minus_Commission','AUR', 'Cancelled_Units']
    
    final_columns = ['Item_Name', 'SKU', 'GMV', 'Units_Sold', 'Orders', 'Offer_Pageviews','Offer_Conversion', 'Item_id', 'Base_Item_Id', 'Item_Pageviews','Item_Conversion', 'Auth_Sales', 'Cancelled_Sales', 'Refund_Sales','Department', 'Brand', 'Listing_Quality_Score', 'GMV_Minus_Commission','AUR', 'Cancelled_Units','file_name','run_date','transaction_date']
    
    wal_transaction_cols = wal_transaction_df.columns.to_list()
    
    # Check if all expected columns are present
    if set(expected_cols) == set(wal_transaction_cols):
        print("Walmart MPI Transaction file's : All columns are matched")
 
        #Add file_name column and assgin file_name
        wal_transaction_df['file_name'] = file_name
        
        #Add run_date column
        wal_transaction_df['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        #Add transaction_date column and assign date in yyyy-mm-dd formate
        wal_transaction_df['transaction_date'] = formatted_date
        
        #Rearrange columns
        wal_transaction_df = wal_transaction_df.reindex(columns=final_columns)
        
        #Generate modified CSV file
        wal_transaction_df.to_csv(f"/home/dstdw/Marketplace/Walmart_transaction/modified_files/{prev_date}/mod_{file_name}",sep='|',index=False)
    else:
        # Find the mismatched columns
        mismatched_columns = set(expected_cols) ^ set(wal_transaction_cols)
        logging.info(f'File_name : {file_name} , Mismatched_cols : {mismatched_columns} , S3_path : {destination_file_path}')
        print("Walmart MPI Transaction file's mismatched columns:", mismatched_columns)
        

def read_list_file():
    
    #Read list file and create list_df
    list_df = pd.read_csv(f'/home/dstdw/Marketplace/Walmart_transaction/list_files/wmpi_transaction_list_file_{prev_date}.txt',header=None)
    
    #Create a list of files and store it into file_list variable
    file_list=list_df[0].to_list()
    
    #Take total file count that to be modified
    total_file_count = len(file_list)
    
    print('Total number of files:',total_file_count)
    
    print(file_list)
    
    #call wal_transaction_modification file function and pass the file name as an argument
    for i in range(len(file_list)):
        modify_mpi_transaction(file_list[i])


if __name__ == "__main__":
    main(sys.argv[1:])