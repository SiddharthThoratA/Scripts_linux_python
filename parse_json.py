# -*- coding: utf-8 -*-
"""
Created on Sun Jan 30 18:29:24 2022

@author: admin
"""

import psycopg2, json, pandas as pd

def parse_json():

    con = psycopg2.connect(dbname= 'postgres', host='172.21.1.86', port = '5432', user= 'jared', password= 'init#1234')
    
    print("hey")
        
    cursorca = con.cursor()
    
    cursorca.execute("""select
	wo.wal_order_json 
from
	b2corders.walmart_orders wo;""")
    
    colnames = [desc[0] for desc in cursorca.description]
    
    rv = cursorca.fetchall()
    
    print(len(rv))
    
    result1 = pd.DataFrame()
    
    for j in range(len(rv)):
        print(rv[j][0])
        data1 = json.dumps(rv[j][0])
        data = json.loads(data1)
        for i in range(len(data['orderLines']['orderLine'])):
            df1 = pd.json_normalize(data)
            df2 = pd.json_normalize(data['orderLines']['orderLine'][i])
            df3 = pd.json_normalize(data['orderLines']['orderLine'][i]['charges']['charge'])
            df4 = pd.json_normalize(data['orderLines']['orderLine'][i]['orderLineStatuses']['orderLineStatus'])
            df1['key'] = 1
            df2['key'] = 1
            df3['key'] = 1
            df4['key'] = 1
            result = pd.merge(pd.merge(pd.merge(df1, df2, on = 'key'), df3, on = 'key'), df4, on = 'key')
            print(result.head())
            result1 = result1.append(result)
            
    result1 = result1.drop(['orderLineStatuses.orderLineStatus', 'orderLines.orderLine', 'charges.charge', 'key'], axis=1)    
    result1.to_csv('C:\\Users\\admin\\Downloads\\out.csv', index = False)
    

if __name__ == '__main__':
    parse_json()
