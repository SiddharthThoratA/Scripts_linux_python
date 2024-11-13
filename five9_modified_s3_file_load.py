import os
import logging
import sys, getopt
import datetime
import csv
import psycopg2
import shutil
import json
import time
import pandas as pd
#import datetime
from datetime import datetime, timedelta

# Setup logging
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#logging.basicConfig(filename='/home/dstdw/five9/log/mismatched_files_.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#run_dt=''

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hd:c:", ["date=", "config="])
    except getopt.GetoptError:
        print('mssc_api_extraction.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('mssc_api_extraction.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : mssc_api_extraction.py -c <config_file> -d <rundate>')
                sys.exit()

    print('config file is :', config_file)
    print('run date is :', run_dt)
    
    #Make variable as global so it can be used any where in the script
    listOfGlobals = globals()
    listOfGlobals['run_dt'] = run_dt

    logging.basicConfig(filename=f'/home/dstdw/five9/log/column_mismatched_file_{run_dt}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    #Call read_config function to read parameter file
    read_config(config_file)

def read_config(config_file):
    conf_file = config_file
    fileobj = open(conf_file)
    #Create a dictionary for parameters
    params = {}
    for line in fileobj:
        line = line.strip()
        if not line.startswith('#'):
            conf_value = line.split('=')
            if len(conf_value) == 2:
                params[conf_value[0].strip()] = conf_value[1].strip()
    fileobj.close()

    #params.update(AUTH_PARAMETERS = parameters_file)
    print('Authentication paramerters :',params)

    listOfGlobals = globals()
    listOfGlobals['params'] = params
    
    #call this function to use values from the created dictionary and create directory in Unix
    ex_param_file(params)

def ex_param_file(params):
    #print('inside ex_sql_file()')
        
    if params['OUT_FILE_LOC']:
        ofilepath = params['OUT_FILE_LOC'].strip()
        ofilepath = ofilepath.replace('RUN_DATE',run_dt)
        print("ofilepath is - ",ofilepath)
        
        #create a date folder in given path
        try:
            os.mkdir(ofilepath)
        except OSError:
            print ("Creation of the directory %s failed" % ofilepath)
        else:
            print ("Successfully created the directory %s " % ofilepath)
            
    if params['TIGGER_FILE_LOC']:
        trigger_path = params['TIGGER_FILE_LOC'].strip()
        trigger_path = trigger_path.replace('RUN_DATE',run_dt)
        print("trigger File path is - ",trigger_path)
       
        #create a date folder in given path
        try:
            os.mkdir(trigger_path)
        except OSError:
            print ("Creation of the directory %s failed" % trigger_path)
        else:
            print ("Successfully created the directory %s " % trigger_path)
           
    #call this function to call all other report modification functions       
    call_report_generate_func()
            
#This function will call total 7 function one by one to modified Five9 reports
def call_report_generate_func():

    #calling ACD_Queue_Quality_of_Service report 
    #ACD_Queue_Quality_of_Service_report()
    #
    ##calling ACD_Queue_Time_By_Campaign_Queue report 
    #ACD_Queue_Time_By_Campaign_Queue_report()
    #
    ##calling Agent_Productivity_by_Campaign_report
    #Agent_Productivity_by_Campaign_report()
    #
    ##calling Agent_Productivity_by_Disposition_report
    #Agent_Productivity_by_Disposition_report()
    #
    ##calling Inbound_Call_Log_report
    #Inbound_Call_Log_report()
    #
    ##calling Agent_Reason_Code_Summary_report
    #Agent_Reason_Code_Summary_report()
    #
    ##calling Agent_State_Summary_by_State_report
    #Agent_State_Summary_by_State_report()
    
    #Calling call_log_report
    call_log_report()
    
    print('state : complete')
    
#1.ACD_Queue_Quality_of_Service_report            
def ACD_Queue_Quality_of_Service_report():
    report_name = 'ACD_Queue_Quality_of_Service.csv'
    acd_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
    
    expected_cols = ['SKILL', 'DATE', 'CALLS', 'Average SPEED OF ANSWER', 'SERVICE LEVEL (%rec)', 'Max QUEUE WAIT TIME', 'Min QUEUE WAIT TIME', 'Average HANDLE TIME', 'FIRST CALL RESOLUTION count', 'ABANDONED count', 'Max TIME TO ABANDON', 'Min TIME TO ABANDON', 'Average TIME TO ABANDON', 'SOA MIN/THRESHOLD (sec)']
        
    final_cols = ['SKILL', 'DATE', 'CALLS', 'Average SPEED OF ANSWER', 'SERVICE LEVEL (%rec)', 'Max QUEUE WAIT TIME', 'Min QUEUE WAIT TIME', 'Average HANDLE TIME', 'FIRST CALL RESOLUTION count', 'ABANDONED count', 'Max TIME TO ABANDON', 'Min TIME TO ABANDON', 'Average TIME TO ABANDON', 'SOA MIN/THRESHOLD (sec)','file_name','run_date']

    acd_file_cols = acd_df.columns.to_list()
    #print(acd_file_cols)
    
    # Check if all expected columns are present
    if set(expected_cols) == set(acd_file_cols):
        print("ACD_Queue_Quality_of_Service_report : All columns are matched")
        
        #Added file_name column and assinged file name
        acd_df['file_name'] = f'ACD_Queue_Quality_of_Service_{run_dt}.csv'
        
        #Added run_date column and assinged current run date & time
        acd_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        #Arrange columns as per the final_cols list
        acd_df = acd_df.reindex(columns=final_cols)
        
        #Genereate csv file in modified file folder
        acd_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
        
        #Call trigger generation function and pass report_name as an argument
        generate_trigger_files(report_name)

    else:
        # Find the mismatched columns
        mismatched_columns = set(expected_cols) ^ set(acd_file_cols)
        logging.info(f'File_name : {report_name} , Mismatched_cols : {mismatched_columns} , S3_path : desototech/CSR_Reports/five9/{run_dt}')
        #logging.error("ACD_Queue_Quality_of_Service mismatched columns: %s", mismatched_columns)
        print("ACD_Queue_Quality_of_Service mismatched columns:", mismatched_columns)

#2.ACD_Queue_Time_By_Campaign_Queue_report
def ACD_Queue_Time_By_Campaign_Queue_report():
    report_name = 'ACD_Queue_Time_By_Campaign,_Queue.csv'
    acd_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
   
    expected_cols = ['CAMPAIGN','SKILL','CALLS','IVR TIME','QUEUE WAIT TIME','TALK TIME','HOLD TIME','PARK TIME','CONSULT TIME','CONFERENCE TIME','AFTER CALL WORK TIME']
    
    final_cols = ['CAMPAIGN','SKILL','CALLS','IVR TIME','QUEUE WAIT TIME','TALK TIME','HOLD TIME','PARK TIME','CONSULT TIME','CONFERENCE TIME','AFTER CALL WORK TIME','file_name','run_date']
    
    acd_file_cols = acd_df.columns.to_list()
    
    # Check if all expected columns are present
    if set(expected_cols) == set(acd_file_cols):
        print("ACD_Queue_Time_By_Campaign_Queue : All columns are matched")
        
        acd_df['file_name'] = f'ACD_Queue_Time_By_Campaign,_Queue_{run_dt}.csv'
        acd_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        acd_df = acd_df.reindex(columns=final_cols)
        
        acd_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
        
        #Call trigger generation function and pass report_name as an argument
        generate_trigger_files(report_name)

    else:
        # Find the mismatched columns
        mismatched_columns = set(expected_cols) ^ set(acd_file_cols)
        logging.info(f'File_name : {report_name} , Mismatched_cols : {mismatched_columns} , S3_path : desototech/CSR_Reports/five9/{run_dt}')
        #logging.error("ACD_Queue_Time_By_Campaign_Queue mismatched columns: %s", mismatched_columns)
        print("ACD_Queue_Time_By_Campaign_Queue mismatched columns:", mismatched_columns)

#3.Agent_Productivity_by_Campaign_report
def Agent_Productivity_by_Campaign_report():
    report_name = 'Agent_Productivity_by_Campaign.csv'
    apc_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
    
    expected_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','CAMPAIGN','CALLS count','HANDLE TIME','Average HANDLE TIME','TALK TIME','Average TALK TIME','AFTER CALL WORK TIME','Average AFTER CALL WORK TIME']
    
    final_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','CAMPAIGN','CALLS count','HANDLE TIME','Average HANDLE TIME','TALK TIME','Average TALK TIME','AFTER CALL WORK TIME','Average AFTER CALL WORK TIME','file_name','run_date']
    
    apc_file_cols = apc_df.columns.to_list()
    
    # Check if all expected columns are present
    if set(expected_cols) == set(apc_file_cols):
        print("Agent_Productivity_by_Campaign : All columns are matched")
        
        apc_df['file_name'] = f'Agent_Productivity_by_Campaign_{run_dt}.csv'
        apc_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        apc_df = apc_df.reindex(columns=final_cols)
        
        apc_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
        
        #Call trigger generation function and pass report_name as an argument
        generate_trigger_files(report_name)

    else:
        # Find the mismatched columns
        mismatched_columns = set(expected_cols) ^ set(apc_file_cols)
        logging.info(f'File_name : {report_name} , Mismatched_cols : {mismatched_columns} , S3_path : desototech/CSR_Reports/five9/{run_dt}')
        #logging.error("Agent_Productivity_by_Campaign mismatched columns: %s", mismatched_columns)
        print("Agent_Productivity_by_Campaign mismatched columns:", mismatched_columns)

#4.Agent_Productivity_by_Disposition_report
def Agent_Productivity_by_Disposition_report():
    report_name = 'Agent_Productivity_by_Disposition.csv'
    apd_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
    
    
    expected_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','DISPOSITION','CALLS count','HANDLE TIME','Average HANDLE TIME','TALK TIME','Average TALK TIME','AFTER CALL WORK TIME','Average AFTER CALL WORK TIME']
    
    final_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','DISPOSITION','CALLS count','HANDLE TIME','Average HANDLE TIME','TALK TIME','Average TALK TIME','AFTER CALL WORK TIME','Average AFTER CALL WORK TIME','file_name','run_date']
    
    apd_file_cols = apd_df.columns.to_list()
    
    # Check if all expected columns are present
    if set(expected_cols) == set(apd_file_cols):
        print("Agent_Productivity_by_Disposition : All columns are matched")
        
        apd_df['file_name'] = f'Agent_Productivity_by_Disposition_{run_dt}.csv'
        apd_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        apd_df = apd_df.reindex(columns=final_cols)
        
        apd_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
        
        #Call trigger generation function and pass report_name as an argument
        generate_trigger_files(report_name)

    else:
        # Find the mismatched columns
        mismatched_columns = set(expected_cols) ^ set(apd_file_cols)
        logging.info(f'File_name : {report_name} , Mismatched_cols : {mismatched_columns} , S3_path : desototech/CSR_Reports/five9/{run_dt}')
        #logging.error("Agent_Productivity_by_Disposition mismatched columns: %s", mismatched_columns)
        print("Agent_Productivity_by_Disposition mismatched columns:", mismatched_columns)


#5.Inbound_Call_Log_report
def Inbound_Call_Log_report():
    report_name = 'Inbound_Call_Log.csv'
    ICL_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
        
    expected_cols = ['CALL ID','DATE','TIMESTAMP','CAMPAIGN','SKILL','AGENT','AGENT NAME','DISPOSITION','ANI','CUSTOMER NAME','DNIS','CALL TIME','BILL TIME (ROUNDED)','IVR TIME','IVR PATH','QUEUE WAIT TIME','RING TIME','TALK TIME','HOLD TIME','PARK TIME','AFTER CALL WORK TIME','TRANSFERS','CONFERENCES','HOLDS','ABANDONED','RECORDINGS']
    
    final_cols = ['CALL ID','DATE','TIMESTAMP','CAMPAIGN','SKILL','AGENT','AGENT NAME','DISPOSITION','ANI','CUSTOMER NAME','DNIS','CALL TIME','BILL TIME (ROUNDED)','IVR TIME','IVR PATH','QUEUE WAIT TIME','RING TIME','TALK TIME','HOLD TIME','PARK TIME','AFTER CALL WORK TIME','TRANSFERS','CONFERENCES','HOLDS','ABANDONED','RECORDINGS','file_name','run_date']
    
    ICL_file_cols = ICL_df.columns.to_list()
    
    # Check if all expected columns are present
    if set(expected_cols) == set(ICL_file_cols):
        print("Inbound_Call_Log : All columns are matched")
        
        ICL_df['file_name'] = f'Inbound_Call_Log_{run_dt}.csv'
        ICL_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        ICL_df = ICL_df.reindex(columns=final_cols)
        
        ICL_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
        
        #Call trigger generation function and pass report_name as an argument
        generate_trigger_files(report_name)

    else:
        # Find the mismatched columns
        mismatched_columns = set(expected_cols) ^ set(ICL_file_cols)
        logging.info(f'File_name : {report_name}, Mismatched_cols : {mismatched_columns}, S3_path : desototech/CSR_Reports/five9/{run_dt}')
        #logging.error("Inbound_Call_Log mismatched columns: %s", mismatched_columns)
        print("Inbound_Call_Log mismatched columns:", mismatched_columns)


#6.Agent_Reason_Code_Summary_report
def Agent_Reason_Code_Summary_report():
    report_name = 'Agent_Reason_Code_Summary.csv'
    arcs_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
        
    expected_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','Away From Desk / AGENT STATE TIME','Break / AGENT STATE TIME','Follow-Up Work / AGENT STATE TIME','Meal / AGENT STATE TIME','Meeting / AGENT STATE TIME','Not Ready / AGENT STATE TIME','Out of Queue / AGENT STATE TIME','System / AGENT STATE TIME','Task Completion / AGENT STATE TIME','Training / AGENT STATE TIME']
    
    final_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','Away From Desk / AGENT STATE TIME','Break / AGENT STATE TIME','Follow-Up Work / AGENT STATE TIME','Meal / AGENT STATE TIME','Meeting / AGENT STATE TIME','Not Ready / AGENT STATE TIME','Out of Queue / AGENT STATE TIME','System / AGENT STATE TIME','Task Completion / AGENT STATE TIME','Training / AGENT STATE TIME','file_name','run_date']
    
    arcs_file_cols = arcs_df.columns.to_list()
    
    #Check if all expected columns are present
    missing_columns = [col for col in arcs_file_cols if col not in expected_cols]

    if not missing_columns:
        print("Agent_Reason_Code_Summary_report : All columns are matched")
        
        arcs_df['file_name'] = f'Agent_Reason_Code_Summary_{run_dt}.csv'
        arcs_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        arcs_df = arcs_df.reindex(columns=final_cols)
        
        arcs_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
        
        #Call trigger generation function and pass report_name as an argument
        generate_trigger_files(report_name)
         
    else:
        logging.info(f'File_name : {report_name}, Mismatched_cols : {missing_columns}, S3_path : desototech/CSR_Reports/five9/{run_dt}')
        #logging.error("Agent_Reason_Code_Summary_report mismatched columns: %s", missing_columns)
        print("Agent_Reason_Code_Summary_report Mismatched columns. Missing columns:", missing_columns)
        
        
#7.Agent_State_Summary_by_State_report
def Agent_State_Summary_by_State_report():
    report_name = 'Agent_State_Summary_by_State.csv'
    assr_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
        
    expected_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','After Call Work / AGENT STATE TIME','Not Ready / AGENT STATE TIME','On Call / AGENT STATE TIME','On Voicemail / AGENT STATE TIME','Ready / AGENT STATE TIME','Ringing / AGENT STATE TIME']
    
    final_cols = ['AGENT GROUP','AGENT','AGENT FIRST NAME','AGENT LAST NAME','After Call Work / AGENT STATE TIME','Not Ready / AGENT STATE TIME','On Call / AGENT STATE TIME','On Voicemail / AGENT STATE TIME','Ready / AGENT STATE TIME','Ringing / AGENT STATE TIME','file_name','run_date']
    
    assr_file_cols = assr_df.columns.to_list()
    
    #Check if all expected columns are present
    missing_columns = [col for col in assr_file_cols if col not in expected_cols]

    if not missing_columns:
        print("Agent_State_Summary_by_State_report : All columns are matched")
        
        assr_df['file_name'] = f'Agent_State_Summary_by_State_{run_dt}.csv'
        assr_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        assr_df = assr_df.reindex(columns=final_cols)
        
        assr_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
        
        #Call trigger generation function and pass report_name as an argument
        generate_trigger_files(report_name)
         
    else:
        logging.info(f'File_name : {report_name}, Mismatched_cols : {missing_columns}, S3_path : desototech/CSR_Reports/five9/{run_dt}')
        #logging.error("Agent_State_Summary_by_State_report mismatched columns: %s", missing_columns)
        print("Agent_State_Summary_by_State_report Mismatched columns. Missing columns:", missing_columns)


#8.call_log report
def call_log_report():
    report_name = 'Call_Log.csv'
    call_log_df = pd.read_csv(f'/home/dstdw/five9/delta_files/{run_dt}/{report_name}',sep = ',')
    
    
    call_log_df['file_name'] = f'Call_Log_{run_dt}.csv'
    call_log_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    call_log_df.to_csv(f'/home/dstdw/five9/modified_files/{run_dt}/mod_{report_name}',sep = ',',index=False)
    
    #Call trigger generation function and pass report_name as an argument
    generate_trigger_files(report_name)


#Trigger file generation function
def generate_trigger_files(file_name):
    report_name = file_name.split('.')[0]
    trigger_file_path = '/home/dstdw/five9/trigger_files'
    empty_df = pd.DataFrame()
    #Create empty trigger file
    empty_df.to_csv(f'{trigger_file_path}/{run_dt}/t_{report_name}_{run_dt}.txt',index=False)
    

if __name__ == "__main__":
    main(sys.argv[1:])