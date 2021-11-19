import requests
import random
import time
import lxml.html
import pandas as pd
from bs4 import BeautifulSoup
import csv
import numpy as np

def export_csv(data,filename,mode):
    with open(filename,mode=mode,encoding='utf8') as csvfile:
        fieldnames = list(data.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        writer.writerow(data) 

class ListingPage:
    
    def __init__(self, page_number, proxie):
        self.main_url = f'https://www.checkraka.com/condo/?quicksearch_order=update%2CDESC&page={page_number}'
        self.proxie = proxie
        self.usable = True
        
    def request(self,min_random_time = 2):
        time.sleep(random.random()+min_random_time)
        try_ = 0
        while True:
            if try_ > 50:
                export_csv('exceed_trial','requests_error.csv','a')
                break
            try:
                self.resp = requests.get(self.main_url, headers = {"User-Agent":"Mozilla/5.0"},  
                                         proxies = {'http':'http://'+self.proxie, 'https':'https://'+self.proxie},
                                        timeout = 15)
                if self.resp.status_code == 200:
                    break
                elif self.resp.status_code == 500:
                    export_csv({'error':'status_code_'+str(self.resp.status_code)},'requests_error.csv','a')
                    time.sleep(180)
                    continue
                elif self.resp.status_code == 503:
                    export_csv({'error':'status_code_'+str(self.resp.status_code)},'requests_error.csv','a')
                    time.sleep(1800)
                    continue
                elif self.resp.status_code == 400:
                    export_csv({'error':'status_code_'+str(self.resp.status_code)},'requests_error.csv','a')
                    time.sleep(1800)
                    continue
                else:
                    export_csv({'error':'status_code_'+str(self.resp.status_code)},'requests_error.csv','a')
                    break
            except requests.exceptions.ConnectionError:
                time.sleep(np.random.randint(1,10))
                export_csv({'error':'Connection_error'},'requests_error.csv','a')
                try_+=1
            except requests.exceptions.ProxyError:
                time.sleep(np.random.randint(1,10))
                export_csv({'error':'ProxyError'},'requests_error.csv','a')
                try_+=1
            except requests.exceptions.Timeout:
                time.sleep(np.random.randint(1,10))
                export_csv({'error':'Timeout'},'requests_error.csv','a')
                try_+=1
    def get_data(self):
        self.request()
        self.cont_ = lxml.html.fromstring(self.resp.content.decode('utf-8','ignore'))

        for i in range(1,15):
            self.data_ = {'name':np.nan, 'link':np.nan, 'company':np.nan, 'area':np.nan,
                    'transportation':np.nan, 'address':np.nan, 'geodata':np.nan}        
            self.get_name(i)
            self.get_link(i)
            self.get_company(i)
            self.get_area(i)
            self.get_company(i)
            self.get_area(i)
            self.get_transportation(i)
            self.get_address(i)
            self.get_geodata(i)
            yield self.data_
    
    def get_name(self,i):
        try:
            name = self.cont_.xpath(f'/html/body/div[1]/div[3]/div[4]/ul/li[1]/div[3]/div[{i}]/div/ul[1]/li[2]/ul/li[1]/div[2]/a')
            name = name[0].text
            self.data_['name'] = name
        except IndexError:
            pass
        
    def get_link(self,i):
        try:
            link = self.cont_.xpath(f'/html/body/div[1]/div[3]/div[4]/ul/li[1]/div[3]/div[{i}]/div/ul[1]/li[2]/ul/li[1]/div[2]/a')
            link = link[0].get('href')
            self.data_['link'] = link
        except IndexError:
            pass
        
    def get_company(self,i):
        try:
            company = self.cont_.xpath(f'/html/body/div[1]/div[3]/div[4]/ul/li[1]/div[3]/div[{i}]/div/ul[1]/li[2]/ul/li[2]/div[2]/a')
            company = company[0].text
            self.data_['company'] = company
        except IndexError:
            pass
    
    def get_area(self,i):
        try:
            area = self.cont_.xpath(f'/html/body/div[1]/div[3]/div[4]/ul/li[1]/div[3]/div[{i}]/div/ul[1]/li[2]/ul/li[4]/div[2]')
            area = area[0].text.strip()
            self.data_['area'] = area
        except IndexError:
            pass
        
    def get_transportation(self,i):
        try:
            transportation = self.cont_.xpath(f'/html/body/div[1]/div[3]/div[4]/ul/li[1]/div[3]/div[{i}]/div/ul[1]/li[2]/ul/li[5]/div[2]/a')
            transportation = transportation[0].text
            self.data_['transportation'] = transportation
        except IndexError:
            pass
        
    def get_address(self,i):
        try:
            address = self.cont_.xpath(f'/html/body/div[1]/div[3]/div[4]/ul/li[1]/div[3]/div[{i}]/div/ul[1]/li[2]/ul/li[6]/div[2]/text()')
            address = address[0].strip()
            self.data_['address'] = address
        except IndexError:
            pass
        
    def get_geodata(self,i):
        try:
            geodata = self.cont_.xpath(f'/html/body/div[1]/div[3]/div[4]/ul/li[1]/div[3]/div[{i}]/div/ul[1]/li[2]/ul/li[6]/div[2]/span/span/a')
            geodata =  geodata[0].get('href')
            self.data_['geodata'] = geodata
        except IndexError:
            pass

if __name__ == "__main__":
    instant_proxies = pd.read_excel('instant_proxies.xlsx',names=['proxie'])
    proxie = instant_proxies['proxie'][np.random.randint(len(instant_proxies))]
    
    for i in range(1,179):
        proxie = instant_proxies['proxie'][np.random.randint(len(instant_proxies))]
            scaper = ListingPage(i, proxie)
            print(scaper.main_url)
            for data in scaper.get_data():
                print(data)
                if write == 1:
                    export_csv(data, 'checkraka_condo.csv', 'w')
                    write = 0
                else:
                    export_csv(data, 'checkraka_condo.csv', 'a')
