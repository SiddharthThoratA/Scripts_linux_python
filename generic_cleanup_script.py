import os
import time
from datetime import datetime, timedelta, date
import subprocess
import sys, getopt
import re
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *

current_time = datetime.now()
run_dt = datetime.now().strftime('%Y%m%d')
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
log_dir = r'D:\\oracle\\cleanup\\logs'  
script_path = r'D:\\oracle\\cleanup'
file_path = f"{script_path}\\cleanup_files_folders_path.txt" 
# create a log file with rundate

sys.stdout = open(f"{log_dir}\\generic_cleanup_script_{run_dt}.log", 'w')
sys.stderr = open(f"{log_dir}\\generic_cleanup_script_{run_dt}.log", 'a')
 
os.chdir(script_path)
print(f"--------------------------------------------------------------------------------------------------------")
print(f"-------------------------------- Start_time : {start_time}----------------------------------------------")
print(f"--------------------------------------------------------------------------------------------------------")

with open(file_path, "r") as file:
    for line in file:
        # Split the line into path, days, and name using space as the delimiter
        name, path, days = line.strip().split()    
        if os. path. exists(path):
            process_name = name
            print(f"********************************** {process_name}**********************************")
            root_file_path = path
            print("removing file from directory : ",root_file_path)
            days_to_delete = days
            print("Days to delete : ",days_to_delete)
            #find out the date based on days_to_delete
            date_to_delete = (current_time - timedelta(days=int(days))).strftime('%Y%m%d')
            print('Date to detete :',date_to_delete)
            print("----------------------------------------------------------------------------------------------------")
            print('\n')
            print("================================================================================================")
            print("Below list of files and folders will be deleted")
            #print("================================================================================================")
            for (root, dirs, file) in os.walk(root_file_path):
                for file in file:
                    #join file path with filename
                    file_path = os.path.join(root, file)
                    #findout last modification time of a file
                    modification_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    local_modification_time = modification_time.strftime('%Y%m%d')
                    #delete those files whose modification time is less than days specified in cleanup_script
                    if local_modification_time < date_to_delete:
                       print(file_path)
                       os.remove(file_path)
            print("----------------------------------------------------------------------------------------------------")
            
            #delete empty directories
            #print('\n')
            print("================================================================================================")
            print("following empty directories will be deleted")
            print("================================================================================================")
            for (root, dirs, file) in os.walk(root_file_path):
                for dir in dirs:
                    empty_dir_path = os.path.join(root, dir)
                    if os.listdir(empty_dir_path) == []:
                        print(empty_dir_path)
                        os.rmdir(empty_dir_path)      
                
        else:
            #print('\n')
            print(f"Path {path} does not exist")
            
print("Cleanup script has been completed")  
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
create_script_status_file('generic_cleanup_script','daily','Cleanup','CBIindia',start_time,datetime.now().strftime('%Y%m%d%H%M%S'))      