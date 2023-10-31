from pandas.core.frame import DataFrame
import scrapy
import pandas as pd
import re
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.web._newclient import ResponseNeverReceived
from twisted.internet.error import ConnectionRefusedError
import logging
from bs4 import BeautifulSoup
import json
from cleantext import clean

PLACE_ID='place_id'
WEBSITE = 'website'
ADDRESS = 'Address'
STATUS = 'Status'
INDEX='Index'
UNIQUE_ID='unique_id'

end_address_marker = re.compile(r'(.{0,50})\b(?:\d+\s(?:floor|office|building|street)\s[\w\s]+?,\s+[\w\s]+?,?\s*(?:nsw|new\s+south\s+wales|sydney|syd)\b.*?\b\d{4}\b)(.{0,50})', re.IGNORECASE)
# address_marker = re.compile(r"(\b(?:nsw|new\s+south\s+wales|sydney|SYD)\b[.,\s\w]+?\b\d{4}\b)", re.IGNORECASE)  
address_marker = re.compile(r"(\b(?:head\s+office|office|suite|level|street|st|road|rd|ave|avenue|nsw|new\s+south\s+wales|sydney|syd)\b[.,\s\w-]+?\b\d{4}\b)", re.IGNORECASE)
target_keywords = ['New South Wales', 'NSW', 'Sydney', 'Syd', 'street','st','office','building']
tags_to_filter = ['p', 'span', 'a', 'address']


#logging
logging.basicConfig(filename='places-api-sydney-address-crawl.log', filemode='a')

# list to append all website,emailid,address
place_id_list=[]
website_url_list=[]
address_list=[]
status_list=[]
index_list=[]

def read_input_file(filename,chunksize=None):

    columns=[PLACE_ID, WEBSITE]
    if chunksize:
        # Read the entire file
        df=pd.read_csv(filename)
        for i in range(0, len(df), int(chunksize)):
            yield df.iloc[i:i+int(chunksize)]
    else:
        return pd.read_csv(filename,sep=' ')

def write_data_file(filename,data_to_append):
    try:
        with open(filename, "r") as json_file:
            data = json.load(json_file)
    except (json.JSONDecodeError, FileNotFoundError):
        data = []
    data.append(data_to_append)
    with open(filename, "w") as json_file:
        json.dump(data, json_file, indent=4)

def clean_text(text):
    clean_except_tags = re.compile('(?!(\/?p|\/?span|\/?a|\/?address))<.*?>')
    filtered_html_text = re.sub(clean_except_tags, '', text)  # Removes all HTML tags except those specified by the clean_except_tags pattern.
    filtered_html_text=filtered_html_text.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip().replace('  ',' ').replace('  ',' ') # Replaces carriage returns, newlines, and tabs with spaces.
    filtered_html_text=filtered_html_text.strip().encode('ascii', 'ignore').decode('ascii') # Strips extra spaces and encodes the text to ASCII, ignoring non-ASCII characters.
    filtered_html_text= re.sub('<svg.*?>.*?</svg>', '', filtered_html_text, flags=re.DOTALL) # Removes SVG tags and their contents.
    filtered_html_text=re.sub(r'\s+', ' ', filtered_html_text).strip()
    filtered_html_text= re.sub(r'{.*?}', '', filtered_html_text)
    return filtered_html_text

def append_item(index,place_id, website_url, address, status):
    place_id_list.append(str(place_id))
    website_url_list.append(website_url)
    address_list.append(address)
    status_list.append(status)
    index_list.append(index)

def extract_address( address_response ):
    opening_tag_index = address_response.index( ">" , 0 )
    closing_tag_index = address_response.index( "<" , 1 )
    length_addr_repsonse = len(address_response)
    address_string = ""
    space = " " 
    count=0
    
    for char in address_response:
        if char == "<":
            count += 1        
    
    for char in range(count-1):
        if closing_tag_index > 0 :
            opening_tag_index = address_response.index( ">" , opening_tag_index ,length_addr_repsonse)
            closing_tag_index = address_response.index( "<" , closing_tag_index ,length_addr_repsonse)
            address_string = address_string + address_response[ opening_tag_index + 1 : closing_tag_index ] + space
            opening_tag_index += 1
            closing_tag_index += 1
        else:
            break
        
    address_string = address_string.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip().replace('  ',' ').replace('  ',' ')
    return address_string    

def find_address(response, place_id,index, self):

    html_text = str(response.text)        
    website_url = str(response.url).replace('"', '')
    address_markers=set(re.findall(address_marker,html_text))
    raw_text=clean_text(html_text)
    raw_content_filename='raw_content.json'
    try:
        with open(raw_content_filename, "r") as json_file:
            data = json.load(json_file)
    except (json.JSONDecodeError, FileNotFoundError):
        data = []
    data.append({'place_id':place_id,'raw_content':raw_text})
    with open(raw_content_filename, "w") as json_file:
        json.dump(data, json_file, indent=4)
    pattern = r'(.{0,50})(\b(?:' + '|'.join(target_keywords) + r')\b)((.|\n){0,50})'
    matches = re.findall(pattern, raw_text,re.IGNORECASE)
    data_to_append=[]
    if matches:
        data_to_append = {
            'content': ''.join(''.join(match) if isinstance(match, tuple) else match for match in matches),
            "website_url": website_url,
            "place_id":place_id,
        }
    else:
        data_to_append = {
            'content': '',
            "website_url": website_url,
            "place_id":place_id,
        }
    if(len(address_markers)==0): 
        data_to_append['status']='AddressNotFound'
        write_data_file(self.data_file,data_to_append)
        append_item(index,str(place_id), website_url, '--', "AddressNotFound"); 
    else: 
        data_to_append['status']='AddressFound'
        write_data_file(self.data_file,data_to_append)
        for marker in address_markers:
            marker = re.sub(r'<(script|style)(.*?)<\/\1>', '', marker , flags=re.DOTALL)
            if(isinstance(marker, tuple)):
                if marker[0] == '':
                    marker = marker[1]
                else:
                    marker = marker[0]   
            ancestor_text = response.xpath('//*/text()[contains(normalize-space(), "{}")]/../..'.format(marker)).getall()                 
            address_response = ancestor_text[0].strip()
            final_address = extract_address(address_response)
            final_address=clean_text(final_address)
            ## Check the length of this text and take just the max 200 characters preceding the marker
            if(len(final_address) > 500):
                marker_position = final_address.find(marker)
                if marker_position > 0:
                    end_position = marker_position + len(marker) + 1
                    start_posiiton = (marker_position - 200) if (marker_position > 200) else 0
                    final_address = final_address[start_posiiton: end_position]
            logging.debug("Address found - {}".format(final_address))
            append_item(index,str(place_id), website_url, final_address, 'AddressFound')                

class AddressCrawler(scrapy.Spider):
    name='address-spider'
    custom_settings = {
        'RETRY_ENABLED': False,
        'DEPTH_LIMIT': 2
    }

    def start_requests(self):

        logging.info(self.input_file)
        logging.info(self.output_file)
        logging.info(self.data_file)
        logging.info(self.chunksize)

        df_scraping = read_input_file(self.input_file, chunksize=self.chunksize)

        for chunk_number, chunk in enumerate(df_scraping, start=1):
            for index in range( len( chunk[PLACE_ID] ) ):
                place_url = str(chunk[WEBSITE].iloc[index])
                place_id= chunk[PLACE_ID].iloc[index]
                if place_url=="nan" :
                    website_url="---------------"
                    address="---------------"
                    status="WebsiteNotAvailable"
                    append_item(index,place_id, website_url, address, status)
                else :
                    if "http://"  not in place_url and "https://" not in place_url:
                        place_url= "https://"  + place_url

                    yield scrapy.Request(url=place_url, callback=self.parse_home_page, meta={'place_id' : place_id,'index':index}, errback=self.error_callback)
            
    def error_callback(self, failure):
        err_response = 'Unknown'
        request = failure.request
        place_id= request.meta['place_id']
        index=request.meta['index']
            
        if failure.check(HttpError):
            err_response ='HttpError'            
        elif failure.check(DNSLookupError):
            err_response='DNSLookupError'
        elif failure.check(TimeoutError, TCPTimedOutError):
            err_response ='TimeoutError'
        elif failure.check(ResponseNeverReceived):
            err_response='ResponseNeverReceived'      
        elif failure.check(ConnectionRefusedError):
            err_response='ConnectionRefusedError'
           
        logging.error(err_response + ' ' + request.url)      
        append_item(index, str(place_id), request.url, '--', err_response)
       
    def parse_home_page(self, response):
        place_id=response.request.meta['place_id']
        index=response.request.meta['index']
        find_address(response, place_id,index, self)
        allow_patterns = (re.compile(r'about', re.IGNORECASE), re.compile(r'contact|cointact\s*us', re.IGNORECASE), re.compile(r'enquire', re.IGNORECASE), re.compile(r'location|locate|locate\s*us', re.IGNORECASE))
        links = LxmlLinkExtractor(allow=allow_patterns,deny=(r'about(.)*[\/](.)+', r'privacy', r'terms')).extract_links(response)
        for link in links:
            yield response.follow(link.url, callback=self.parse_inner_page, meta={'place_id':place_id,'index':index }, errback=self.error_callback)       

    def parse_inner_page(self, response):
        place_id= response.request.meta['place_id']
        index=response.request.meta['index']
        find_address(response, place_id,index, self)
        
    def close(self, reason):
        # TODO: This seems to be writing the whole file after _every_ crawled page
        logging.info("Writing the crawled addresses to a file")
        df_address_database = pd.DataFrame(list(zip(index_list,place_id_list, website_url_list, address_list, status_list)),
            columns =[INDEX, PLACE_ID, WEBSITE, ADDRESS, STATUS])
        df_address_database.to_csv(self.output_file, sep='|',index=False)
        
