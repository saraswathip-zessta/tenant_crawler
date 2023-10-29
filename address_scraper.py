"""
Created on Thu Nov 18 16:31:42 2021

@author: prity
"""
from postal.parser import parse_address
from postal.expand import expand_address
import json
import pandas as pd
import re
import datetime
import time
import argparse
import csv

# Logger settings.
import logging
import logging.handlers

logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter=logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler=logging.handlers.RotatingFileHandler('mel_post_process.log',backupCount=3 , maxBytes=10000000)
handler.setFormatter(formatter)

logger.handlers.clear()  # Clearing any handlers if present.
logger.addHandler(handler)


# Display settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 1)

#List to store contact details
lst_clean_addr=[]
lst_tenant_id=[]
lst_crawl_link=[]
lst_raw_addr=[]
lst_parsed_addr=[]
lst_status=[]
lst_postcode=[]
lst_city=[]
lst_suburb=[]
lst_state=[]

# List to get stats
lst_total_url=[]
lst_ID_addr_found=[]
lst_ID_error_found=[]
lst_addr_not_found=[]
lst_unique_ID_addr_found=[]

# Start time
strt_time=time.time()

#logger.info('Start time : '+ str(strt_time)) 
print('Start time : '+ str(strt_time)) 

###Parse addr through Libpostal
def convert_json(address):
    new_address = {k: v for (v, k) in address}
    json_address = json.dumps(new_address, sort_keys=True,ensure_ascii = False, indent=1)
    return json_address

def address_parser(address):
    expanded_address = expand_address( address )[0]
    parsed_address = parse_address( expanded_address )
    json_address = convert_json(  parsed_address )
    return json_address

logger.info('*********** POST PROCESSING STARTED ***********')

# Get the arguments from commandline.
parser=argparse.ArgumentParser()
parser.add_argument('input_file_path',help="Please provide Generic_Crawler_File." )
args=parser.parse_args()
input_file_path=args.input_file_path
output_file_path=args.output_file_path

#Read Generic Crawler File
df_scraping = pd.read_csv(input_file_path,sep='|',low_memory = True)

#Remove Duplicates
df_scraping=df_scraping.drop_duplicates()
df_scraping=df_scraping.drop_duplicates(subset=['al_property_id','Address'], keep='first')
# Fill dataframe with empty string if NaN
df_scraping = df_scraping.fillna('')
df_scraping=df_scraping.reset_index(drop=True)


# Total Number of Address Found
for index in range(len(df_scraping['al_property_id'])):
    lst_total_url.append(df_scraping['al_property_id'][index])
    if df_scraping['Status'][index]=='AddressFound':
        ID_addr_found=df_scraping['al_property_id'][index]
        lst_ID_addr_found.append(ID_addr_found)
    elif df_scraping['Status'][index]=='AddressNotFound':
        ID_addr_not_found=df_scraping['al_property_id'][index]
        lst_addr_not_found.append(ID_addr_not_found)
    else:
        ID_error=df_scraping['al_property_id'][index]
        lst_ID_error_found.append(ID_error)


#print("Total number of Unique Tenant ID with Address Not Found Status ::",len(lst_addr_not_found))
#print("Total number of Unique Tenant ID with Error ::",len(lst_ID_error_found))

#logger.info("Total number of Tenant Id crawled ::"+str(len(set(lst_total_url))))
#logger.info("Total number of Address Found Status ::"+str(len(lst_ID_addr_found)))    

#lst_unique_ID_addr_found=set(lst_ID_addr_found) 
#logger.info("Total number of Unique Tenant ID with Address Found Status ::"+str(len(lst_unique_ID_addr_found))) 

# Find common Tenant id for addr match status & addr not match status
lst_temp_cmn_addr_not_found=list(set(lst_ID_addr_found) & set(lst_addr_not_found))
#print(len(lst_temp_cmn_addr_not_found))
# Find common Tenant id for addr match status & error status
lst_temp_cmn_error=list(set(lst_ID_addr_found) & set(lst_ID_error_found))
#print(len(lst_temp_cmn_error))
# Join to form common list
lst_final_status_change=lst_temp_cmn_addr_not_found+lst_temp_cmn_error
#print(len(lst_final_status_change))
#Find those tenand Id and change status
for index in range(len(df_scraping['al_property_id'])):
    if df_scraping['al_property_id'][index] in lst_final_status_change and df_scraping['Status'][index]!='AddressFound':
        df_scraping['Status'][index]='change'


# Remove rows where status changed
df_scraping = df_scraping.loc[df_scraping["Status"] != "change"]
df_scraping=df_scraping.reset_index(drop=True)
    
#Stats for Status : Error / Addr not available
lst_stat_addr_not_found=[]
lst_stat_error=[]
for index in range(len(df_scraping['al_property_id'])):
    if df_scraping['Status'][index]!='AddressFound' and df_scraping['Status'][index]!='AddressNotFound':
        ID_error_found=df_scraping['al_property_id'][index]
        lst_stat_error.append(ID_error_found)
    if df_scraping['Status'][index]=='AddressNotFound':
         addr_not_found=df_scraping['al_property_id'][index]
         lst_stat_addr_not_found.append(addr_not_found)

print("Total number of unique tenant ID where website dosent crawl(Error) ::",len(set(lst_stat_error)))
print("Total number of unique tenant ID where addr not available::",len(set(lst_stat_addr_not_found)))
print("Total number of Tenant Id crawled ::",len(set(lst_total_url)))
print("Total number of Address Found Status ::",len(lst_ID_addr_found))    
lst_unique_ID_addr_found=set(lst_ID_addr_found) 
print("Total number of Unique Tenant ID with Address Found Status ::",len(lst_unique_ID_addr_found)) 


# Regex to filter only clean address(Melborne)
re_clean_address = r"(\d+\s\w+\s\w+\s\bStreet\b|\d+\s\w+\s\w+\bSt\b|\d+\s\w+\s\w+\bRoad\b|\bLevel\b[,]?\s\d+|\bUnit\b[,]?\s\d+|\bGPO Box\b[,]?\s\d{4}|\bPO Box\b|\bNSW\b|\bNew\sSouth\sWales\b|\bSydney\b|\bSyd\b)(.*?)(\bNSW\b|\bNew\sSouth\sWales\b|\bSydney\b|\bSyd\b|\bAustralia\b|\bGPO Box\b[,]?\s\d{4}|\bPO Box\b)"

#Cleaning, Parsing, Saperate parse address to different column 
#Cleaning, Parsing, Saperate parse address to different column 
for index in range(len(df_scraping['al_property_id'])):
#for index in range(20):
    if df_scraping['Status'][index]!='AddressNotFound' and df_scraping['Status'][index]!='AddressFound':
       lst_tenant_id.append(df_scraping['al_property_id'][index])
       lst_crawl_link.append(df_scraping['website'][index])
       lst_raw_addr.append(df_scraping['Address'][index])
       lst_status.append(df_scraping['Status'][index])
       lst_clean_addr.append("")
       lst_parsed_addr.append("")
       lst_postcode.append("")
       lst_city.append("")
       lst_suburb.append("")
       lst_state.append("")

      
    if df_scraping['Status'][index]=='AddressNotFound':
       lst_tenant_id.append(df_scraping['al_property_id'][index])
       lst_crawl_link.append(df_scraping['website'][index])
       lst_raw_addr.append(df_scraping['Address'][index])
       lst_status.append(df_scraping['Status'][index])
       lst_clean_addr.append("")
       lst_parsed_addr.append("")
       lst_postcode.append("")
       lst_city.append("")
       lst_suburb.append("")
       lst_state.append("")
      
    if df_scraping['Status'][index]=='AddressFound':
        clean_address=re.findall(re_clean_address,df_scraping.Address[index]) or ['NA']
        for addr in clean_address:
            addr=list(addr)
            addr=re.sub("'", "", str(addr))
            parse_addr=address_parser(addr)
            mel_parse_address=json.loads(parse_addr)
             # Pre-Processing House Number
            if 'postcode' in mel_parse_address:
                
                postcode=mel_parse_address['postcode']
            else:
                 postcode=''
            if 'city' in mel_parse_address:
               
                city=mel_parse_address['city']
            else:
                 city=''
            if 'suburb' in mel_parse_address:
                suburb=mel_parse_address['suburb']
            else:
                 suburb=''
            if 'state' in mel_parse_address:
                state=mel_parse_address['state']
            else:
                 state=''
            lst_tenant_id.append(df_scraping['al_property_id'][index])
            lst_crawl_link.append(df_scraping.website[index])
            lst_raw_addr.append(df_scraping.Address[index])
            lst_status.append(df_scraping['Status'][index])
            lst_clean_addr.append(addr)
            lst_parsed_addr.append(mel_parse_address)
            lst_postcode.append(postcode)
            lst_city.append(city)
            lst_suburb.append(suburb)
            lst_state.append(state)
        
        
  
df_address_database = pd.DataFrame(list(zip(lst_tenant_id ,lst_crawl_link,lst_raw_addr,lst_status,lst_clean_addr,lst_parsed_addr,lst_postcode,lst_city,lst_suburb,lst_state)),
            columns =['al_property_id' , 'website' ,'Address','Status','CleanAddress','Parsed Address','Post Code','City','Suburb','State'])

print(df_address_database.info())

#df_address_database=df_address_database.drop_duplicates(subset=['TenantId', 'CleanAddress'], keep='first')   
df_address_database.to_csv("./"+output_file_path, sep='|',index=False) 


