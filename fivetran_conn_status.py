import os
import sys, getopt
import json
import time
import pandas as pd
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth

#run_dt=''

run_dt = datetime.now().strftime("%Y%m%d")
script_path = f'D:\\dstdw\\ft_connector_status\\'
log_dir = f'D:\\dstdw\\ft_connector_status\\log_files'

sys.stdout = open(f"{log_dir}\\fivetran_conn_status_{run_dt}.log", 'a')

print(f"***********************************************")
print(f"run date is : {run_dt}")
print(f"script path is : {script_path}")
print(f"script start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
#print('\n')

#Function to call API and get response in Json formate
def check_connector_status(connector_id):
    
    #credentials
    api_key = "raSjTva0XMSx7zBk"
    api_secret = "7UEfV4pIMCPw36TCbGp8fknMobTWZX7h"
    a = HTTPBasicAuth(api_key, api_secret)
    
    url = f'https://api.fivetran.com/v1/connectors/{connector_id}'
    
    h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    
    response = requests.get(url, headers=h, auth=a)
    
    res=response.json()
    
    return res

#Function to extract status,id and schema from the Json response
def extract_response():
    
    #['subtlety_illuminate','attributable_visa',] -- HH connectors
    
    connectors_list = ['twilight_olden','bade_archaic','dancer_distension','appeasing_crumb','pushes_rally','curvature_legislator','happier_packed','nickname_dam','extorted_casino','newness_pedigree','sensory_providential','enormous_respectfully','peptic_laud','duties_arthritis','ipso_ratio','boldly_flatter','etc_cyanide','antiquary_settlement','murky_traverse','eligible_speculators','sport_circumstance','confirmed_musty','inevitable_twistable','swiftness_hinted','rheumatoid_direct','certainty_assay','anatomist_hardhat','finely_mandatory','possessor_agrarian','anxiously_unstamped']
    
    #Disabled ft connector list
    #connectors_list =['shutting_thereto','childcare_consequence','sportsman_deeply','spring_risotto']
    
    paused_connectors = []
    
    broken_connectors = []
    
    delayed_connectors = []

    for i in range(len(connectors_list)):
        
        data = check_connector_status(connectors_list[i])
        
        print('***************************************************************')
        #print(data)
        #print('***************************************************************')
        
        connector_id=data['data']['id']
        connector_schema=data['data']['schema']
        print(f"Fivetran connector name :{connector_schema}")
        setup_state=data['data']['status']['setup_state']
        print(f"setup_state :{setup_state}")
        sync_state = data['data']['status']['sync_state']
        print(f"sync_state :{sync_state}")
        update_state = data['data']['status']['update_state']
        print(f"update_state :{update_state}")
        #print('***************************************************************')
        
        
        #print('connector_id is :',connector_id + '\n' + 'Connector_Name :' + f'{connector_schema}' + '\n' + 'Connector_Status :' + f'{sync_status}')
        
        if setup_state == 'broken':
            broken_connectors.append((connector_id, connector_schema, setup_state))
            #print(broken_connectors)    
        if sync_state == 'paused':
            paused_connectors.append((connector_id, connector_schema, sync_state))
        if update_state == 'delayed':
            delayed_connectors.append((connector_id, connector_schema, update_state))
            
    if broken_connectors:
        print("Broken Connectors:")
        for connector in broken_connectors:
            print('*****************************************')
            print(f"Connector ID: {connector[0]},\n Schema: {connector[1]},\n setup_state: {connector[2]}")
            
            message = f"Connector ID: {connector[0]},\n\n Schema: {connector[1]},\n\n setup_state: {connector[2]}"
        
            send_message_to_teams(message)

    if paused_connectors:
        print("Paused Connectors:")
        for connector in paused_connectors:
            print('*****************************************')
            print(f"Connector ID: {connector[0]},\n Schema: {connector[1]},\n sync_state: {connector[2]}")
            
            message = f"Connector ID: {connector[0]},\n\n Schema: {connector[1]},\n\n sync_state: {connector[2]}"
            
            send_message_to_teams(message)

    if delayed_connectors:
        print("Delayed Connectors:")
        for connector in delayed_connectors:
            print('*****************************************')
            print(f"Connector ID: {connector[0]},\n Schema: {connector[1]},\n update_state: {connector[2]}")
            
            message = f"Connector ID: {connector[0]},\n\n Schema: {connector[1]},\n\n update_state: {connector[2]}"
            
            send_message_to_teams(message)
    
    if paused_connectors == [] and broken_connectors == [] and delayed_connectors == []:
        time_list = ['00:00','07:15','09:00']
        #time_list = ['4:17']
        
        # Create a datetime object
        dt = datetime.now()

        # Extract the time component and format it as HH:MM:SS
        time_formatted = dt.time().strftime('%H:%M')
        print('*****************************************')
        print("Current time (HH:MM):", time_formatted)
        
        
        if time_formatted in time_list:
            print('Send message to MS-Teams!')
            message = "All connectors are working fine."
            send_message_to_teams(message)
            
        else:
            print("All connectors are working fine.")
        
        #send_message_to_teams(message)

#Function to send message on MS-Teams
def send_message_to_teams(message):
    
    # Example usage
    #Testing URL
    #webhook_url = "https://o365spi.webhook.office.com/webhookb2/4ec10c2b-8402-4ea2-9af6-4280be5bee2f@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/b743bb62556940888dd442217de8d0ef/329a3b08-1316-452c-a496-6ab47e68127d"  
    
    #Original URL
    #webhook_url = "https://o365spi.webhook.office.com/webhookb2/6e08f17a-ebdd-4140-84ee-fb3291ee1e9c@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/238bc277c2e44889a31418693577a946/329a3b08-1316-452c-a496-6ab47e68127d"
    webhook_url = "https://o365spi.webhook.office.com/webhookb2/6e08f17a-ebdd-4140-84ee-fb3291ee1e9c@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/238bc277c2e44889a31418693577a946/329a3b08-1316-452c-a496-6ab47e68127d/V23eOFMocOP8rXgv33xqOUP7hTECq7gOTL1N5lbvLUqvE1"
    
    headers = {'Content-Type': 'application/json'}

    payload = {
        "text": message
    }

    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print("Message sent successfully to Microsoft Teams!")
    else:
        print(f"Failed to send message to Microsoft Teams. Status code: {response.status_code}")


if __name__ == "__main__":
    extract_response()
