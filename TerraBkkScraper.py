import requests
import random
import time
import lxml.html
import pandas as pd
from bs4 import BeautifulSoup
import csv
import numpy as np

class NoData(Exception):
    pass

class Error502(Exception):
    pass
class Error404(Exception):
    pass

def export_csv(data,filename,mode='a'):
    with open(filename,mode=mode,encoding='utf8') as csvfile:
        fieldnames = list(data.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        writer.writerow(data)


class Call():
    
    def __init__(self,url,maximum_trial = 50, use_lxml = True):
        self.proxies_list = None
        self.url = url
        self.maximum_trial = maximum_trial
        self.use_lxml = use_lxml
        self.try_ = 0
        self.proxies_list = pd.read_excel('instant_proxies.xlsx')['InstantProxies'].loc[30:].tolist()
        
    #def get_proxies(self):
        #resp = requests.get('http://filefab.com/api.php?l=sZLAdwd1U7TTcfUHpCRb9yngtkL18LzTn82jqMZCud4')##vietnam 24
        #x = BeautifulSoup(resp.content.decode('utf-8'), 'html.parser')
        #s = list(x)
        #proxie_list = s[0].split('\n')
        #proxie_list.remove('')
        #proxie_df = pd.DataFrame({'proxies':proxie_list})
        #proxies_list = proxie_df.proxies.tolist()
        #self.proxies_list = []
        
    
    def call(self):
        time.sleep(random.randint(2,4))
        while True:
            if self.try_ >= self.maximum_trial:
                #self.proxies_list = None
                #self.try_ = 0
                export_csv({'proxies_expired'},'current_dot_error.csv')
                raise ExceedTrial
            try:
                #if self.proxies_list == None:
                    #self.get_proxies()
                    #self.try_ = 0
                proxie = random.choice(self.proxies_list)
                proxies = {'http':'http://'+proxie, 'https':'https://'+proxie}
                resp = requests.get(self.url, headers = {"User-Agent":"Mozilla/5.0"}, proxies = proxies,timeout = 15)
                resp.encoding = resp.apparent_encoding
                if resp.status_code == 200:
                    print('calling success')
                    break
                elif resp.status_code == 404:
                    error = {'error 404'}
                    export_csv(error,'current_terra_error.csv')
                    raise Error404
                elif resp.status_code == 502:
                    error = {'error 502'}
                    export_csv(error,'current_dot_error.csv')
                    raise Error502
                elif resp.status_code == 500:
                    error = {'error 500'}
                    export_csv(error,'current_dot_error.csv')
                    raise Error500
            except requests.exceptions.ProxyError:
                self.try_+=1
                print('proxy error')
            except requests.exceptions.ConnectionError:
                print('sleep')
                self.try_+=1
                time.sleep(random.randint(1,10))
                pass
            except requests.exceptions.Timeout:
                print('timeout sleep')
                time.sleep(random.randint(20,30))
                self.try_+=1
                pass
        if self.use_lxml == True:
            self.try_ = 0
            return lxml.html.fromstring(resp.content.decode('utf-8','ignore'))
        elif self.use_lxml == 'both':
            self.try_ = 0
            return lxml.html.fromstring(resp.content.decode('utf-8','ignore')),BeautifulSoup(resp.content.decode('utf-8','ignore'), 'html.parser')
        elif self.use_lxml == False :
            self.try_ = 0
            return BeautifulSoup(resp.content.decode('utf-8','ignore'), 'html.parser')

class TerraBkk:
    
    def __init__(self,url):
        self.url = url
        self.main_ = Call(url).call()
        self.name_ = '-'
        self.link_ = '-'
        self.n_content_ = '-'
        self.price1_ = '-'
        self.price2_ = '-'
        self.detail_dict = {'title':np.nan,'link':np.nan,'location':np.nan,'price':np.nan,'posted_date':np.nan,
                            'ประเภททรัพย์':np.nan, 'ที่ดิน':np.nan, 'พื้นที่ใช้สอย':np.nan, 'จำนวนห้องนอน':np.nan,'จำนวนห้องน้ำ':np.nan,
                            'จำนวนที่จอดรถ':np.nan, 'จำนวนชั้น':np.nan,'lat':np.nan,'lng':np.nan,'detailed_location':np.nan}
    
    def n_content(self):
        try:
            x = self.main_.xpath('//*[@id="property-col"]/div[3]')
            self.n_content_ = len(x[0])
        except:
            NoMainPage
            
    def get_data(self,i):
        self.main(i)
        self.access_link()
        self.detail()
        self.detail2()
        self.detail3()
        self.date()
        self.get_latlng()
        return self.detail_dict
        
    def main(self, i):
        try:
            x = self.main_.xpath('//*[@id="property-col"]/div[3]/div[{}]/div/a/h4'.format(i))
            self.detail_dict['title'] = x[0].text_content()
            x = self.main_.xpath('//*[@id="property-col"]/div[3]/div[{}]/div/a'.format(i))
            self.detail_dict['link'] = x[0].get('href')
            x = self.main_.xpath('//*[@id="property-col"]/div[3]/div[{}]/div/a/p[1]'.format(i))
            self.detail_dict['location'] = x[0].text_content()
            x = self.main_.xpath('//*[@id="property-col"]/div[3]/div[{}]/div/a/div/p'.format(i))
            if len(x) == 1:
                self.detail_dict['price'] = x[0].text_content()
            else:
                self.detail_dict['price'] = x[0].text_content() + '-' + x[1].text_content()
        except IndexError:
            raise NoData('No data in main page')
    
        
    def access_link(self):
        try:
            self.cont_,self.soup_ = Call(self.detail_dict['link'],use_lxml='both').call()
        except:
            raise NoData('No link')

    def detail(self):
        try:
            x = self.soup_.find_all('p',{'class':'col-sm'})
            for i in range(len(x)):
                self.get_detail(x[i].get_text())
        except:
            pass
    def date(self):
        try:
            x = self.soup_.find_all('div',{'class':'item-share row'})
            s = x[0].get_text().strip().split('\n')[0]
            self.detail_dict['posted_date'] = s[s.find('ประกาศเมื่อ')+len('ประกาศเมื่อ'):]
        except:
            pass
    
    def get_detail(self,s):
        for key,_ in self.detail_dict.items():
            if key in s:
                self.detail_dict[key] = s[s.find(key)+len(key):]
    
    def get_latlng(self):
        try:
            x = self.soup_.find_all('div',{'id':'map-canvas'})
            self.detail_dict['lat'] = x[0].get('data-lat')
            self.detail_dict['lng'] = x[0].get('data-lng')
        except IndexError:
            pass

    def detail2(self):
        x = self.soup_.find_all('div', {'class':'col-6 col-sm-4 col-md-3'})
        for i in range(len(x)):
            try:
                key = x[i].find('div',{'class':'title'}).text
                if (key == 'จำนวนห้องน้ำ') or (key == 'จำนวนที่จอดรถ'):
                    item = x[i].find('div',{'class':'ssgldtext border-bottom mb-3'}).text
                    self.detail_dict[key] = item
            except IndexError:
                pass
    
    def detail3(self):
        x = self.cont_.xpath('//*[@id="freepost-content"]/div[4]/div/div[1]/p')
        try:
            self.detail_dict['detailed_location'] = x[0].text_content()
        except IndexError:
            pass

class ScrapeToCsv:
    
    def __init__(self,province_id=1,house_type = 6,start_page=1,final_page=2000):
        self.start_page = start_page
        self.house_type = house_type
        self.final_page = final_page
        self.province_id = province_id
        self.current_ = {'page':'-', 'content':'-'}
        
    def scrape(self):
        for i in range(self.start_page, self.final_page+1):
            self.current_['page'] = i
            self.url = 'https://www.terrabkk.com/freepost/property-list/{}?f-house_type={}&f-post_type=&d-province_id={}'.format(i,self.house_type,self.province_id)
            t = TerraBkk(self.url)
            t.n_content()
            n = t.n_content_
            if n == 0:
                raise NoMainPage('finished')
            for k in range(1,n+1):
                try:
                    self.current_['content'] = k
                    print(self.current_)
                    export_csv(self.current_,'terra_{}_provincelevel_lastscraped3.csv'.format(self.house_type),'w')
                    yield t.get_data(k)
                except NoData:
                    continue
    
    def to_csv(self,filename,mode='a'):
        writed = 0
        for data in self.scrape():
            if mode == 'w' and writed == 0:
                export_csv(data,filename,'w')
                writed = 1
            else:
                export_csv(data,filename,'a')
                print(data)
    
    def update_the_csv(self,filename,mode = 'a'):
        df = pd.read_csv(filename)
        updated_filename = filename[:filename.find('.csv')]+'_updated_file.csv'
        if mode == 'w':
            count = 0
        else:
            count = 1
        for data in self.scrape():
            if data['link'] in df.link.tolist():
                print('finished')
                break
            else:
                if count == 0:
                    export_csv(data,updated_filename,'w')
                    count+=1
                else:
                    export_csv(data,updated_filename,'a')


if __name__ == "__main__":
    scrape=ScrapeToCsv(house_type=7,start_page=9010,final_page=12000,province_id='')
    scrape.to_csv('terra_condo3.csv','a')