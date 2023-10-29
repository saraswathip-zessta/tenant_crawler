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

WEBSITE = 'website'
ADDRESS = 'Address'
STATUS = 'Status'
INDEX='Index'
AL_PROPERTY_ID = 'al_property_id'

end_address_marker = re.compile(r'(.{0,50})\b(?:\d+\s(?:floor|office|building|street)\s[\w\s]+?,\s+[\w\s]+?,?\s*(?:nsw|new\s+south\s+wales|sydney|syd)\b.*?\b\d{4}\b)(.{0,50})', re.IGNORECASE)
address_marker = re.compile(r"(\b(?:nsw|new\s+south\s+wales|sydney|SYD)\b[.,\s\w]+?\b\d{4}\b)", re.IGNORECASE)  
target_keywords = ['New South Wales', 'NSW', 'Sydney', 'Syd', '2000']
tags_to_filter = ['p', 'span', 'a', 'address']


#logging
logging.basicConfig(filename='property-address-crawl.log', filemode='a')

# list to append all website,emailid,address
propertys_id_list=[]
website_url_list=[]
address_list=[]
status_list=[]
index_list=[]

def read_input_file(filename,chunksize=None):
    columns=[AL_PROPERTY_ID, WEBSITE]
    if chunksize:
        # Read the entire file
        df=pd.read_csv(filename)
        for i in range(0, len(df), int(chunksize)):
            yield df.iloc[i:i+int(chunksize)]
    else:
        return pd.read_csv(filename,sep=' ')

def write_output_file(data_to_append):
    filename = "address_matches_sydney_place_details_with_near_search_data2.json"
    try:
        with open(filename, "r") as json_file:
            data = json.load(json_file)
    except (json.JSONDecodeError, FileNotFoundError):
        data = []
    data.append(data_to_append)
    with open(filename, "w") as json_file:
        json.dump(data, json_file, indent=4)

def filter_content(text,tags):
    soup = BeautifulSoup(text, 'html.parser')
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    for tag in soup.find_all(True):
        if tag.name not in tags:
            tag.hidden = True
    for svg in soup.find_all('svg'):
        svg.decompose()
    return soup.prettify()
def append_item(index,al_property_id, website_url, address, status):
    propertys_id_list.append(str(al_property_id))
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

def find_address(response, al_property_id,index):
    for script in response.xpath('//script'):
        script.extract()
    for style in response.xpath('//style'):
        style.extract()
    html_text = str(response.text)        
    website_url = str(response.url).replace('"', '')        
    address_markers = set(re.findall(address_marker, html_text))
    filtered_html_text = filter_content(html_text, tags_to_filter)
    clean = re.compile('<.*?>')
    clean_except_tags = re.compile('(?!(\/?p|\/?span|\/?address))<.*?>')
    filtered_html_text = re.sub(clean_except_tags, '', html_text)
    filtered_html_text = re.sub(clean, '', filtered_html_text)
    filtered_html_text = re.sub(r'\s+', ' ', filtered_html_text)
    filtered_html_text= filtered_html_text.strip()
    filtered_html_text = re.sub(r'\n+', '\n', filtered_html_text)
    filtered_html_text= re.sub('<svg.*?>.*?</svg>', '', filtered_html_text, flags=re.DOTALL)
    pattern = r'(.{0,100})(\b(?:' + '|'.join(target_keywords) + r')\b)((.|\n){0,100})'
    matches = re.findall(pattern, filtered_html_text,re.IGNORECASE)
    combined_string=''
    if matches:
        combined_string = ''.join(str(item) for item in matches)
        combined_string = re.sub(' +', ' ', combined_string)
        combined_string=combined_string.strip().encode('ascii', 'ignore').decode('ascii')
        combined_string=re.sub(r'[^a-zA-Z0-9,\/\+\- ]|(?<! ) {2,}', '', combined_string)
    if(len(address_markers)==0): 
        data_to_append = {
            "matches": combined_string,
            "combined_length":len(combined_string),
            "website_url": website_url,
            "property_id":int(al_property_id),
            "status":'AddressNotFound'
        }
        write_output_file(data_to_append)
        append_item(index,str(al_property_id), website_url, '--', "AddressNotFound"); 
    else: 
        data_to_append = {
            "matches": combined_string,
            "combined_length":len(combined_string),
            "website_url": website_url,
            "property_id":int(al_property_id),
            "status":'AddressNotFound'
        }
        for marker in address_markers:
            write_output_file(data_to_append)
            if(isinstance(marker, tuple)):
                if marker[0] == '':
                    marker = marker[1]
                else:
                    marker = marker[0] 
                    
            ancestor_text = response.xpath('//*/text()[contains(normalize-space(), "{}")]/../..'.format(marker)).getall()                  
            address_response = ancestor_text[0].strip()
            final_address = extract_address(address_response)
            ## Check the length of this text and take just the max 200 characters preceding the marker
            if(len(final_address) > 500):
                marker_position = final_address.find(marker)
                if marker_position > 0:
                    end_position = marker_position + len(marker) + 1
                    start_posiiton = (marker_position - 200) if (marker_position > 200) else 0
                    final_address = final_address[start_posiiton: end_position]
            
            logging.debug("Address found - {}".format(final_address))
            append_item(index,str(al_property_id), website_url, final_address, 'AddressFound')                

class AddressCrawler(scrapy.Spider):
    name='address-spider'
    custom_settings = {
        'RETRY_ENABLED': False,
        'DEPTH_LIMIT': 2
    }

    def start_requests(self):
        logging.info(self.input_file)
        logging.info(self.row_count)
        logging.info(self.output_file)
        df_scraping = read_input_file(self.input_file, chunksize=self.chunksize)
        for chunk_number, chunk in enumerate(df_scraping, start=1):
            for index in range( len( chunk[AL_PROPERTY_ID] ) ):
                property_url = str(chunk[WEBSITE].iloc[index])
                al_property_id = chunk[AL_PROPERTY_ID].iloc[index]
                if property_url=="nan" :
                    website_url="---------------"
                    address="---------------"
                    status="WebsiteNotAvailable"
                    append_item(index,al_property_id, website_url, address, status)
                else :
                    if "http://"  not in property_url and "https://" not in property_url:
                        property_url= "https://"  + property_url

                    yield scrapy.Request(url=property_url, callback=self.parse_home_page, meta={'al_property_id' : al_property_id,'index':index}, errback=self.error_callback)
            
    def error_callback(self, failure):
        err_response = 'Unknown'
        request = failure.request
        al_property_id = request.meta['al_property_id']
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
        append_item(index,str(al_property_id), request.url, '--', err_response)
       
    def parse_home_page(self, response):
        al_property_id=response.request.meta['al_property_id']
        index=response.request.meta['index']
        find_address(response, al_property_id,index)
        allow_patterns = (re.compile(r'about', re.IGNORECASE), re.compile(r'contact', re.IGNORECASE),re.compile(r'enquire', re.IGNORECASE))
        links = LxmlLinkExtractor(allow=allow_patterns,deny=(r'about(.)*[\/](.)+', r'privacy', r'terms')).extract_links(response)
        for link in links:
            yield response.follow(link.url, callback=self.parse_inner_page, meta={'al_property_id':al_property_id,'index':index }, errback=self.error_callback)       

    def parse_inner_page(self, response):
        al_property_id = response.request.meta['al_property_id']
        index=response.request.meta['index']
        find_address(response, al_property_id,index)
        
    def close(self, reason):
        # TODO: This seems to be writing the whole file after _every_ crawled page
        logging.info("Writing the crawled addresses to a file")
        df_address_database = pd.DataFrame(list(zip(index_list,propertys_id_list, website_url_list, address_list, status_list)),
            columns =[INDEX,AL_PROPERTY_ID, WEBSITE, ADDRESS, STATUS])
        df_address_database.to_csv(self.output_file, sep='|',index=False)
        