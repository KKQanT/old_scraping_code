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

class NoContent(Exception):
    pass

class Error502(Exception):
    pass
class Error404(Exception):
    pass

class Call():
    
    def __init__(self,url,maximum_trial = 50, use_lxml = True):
        self.proxies_list = None
        self.url = url
        self.maximum_trial = maximum_trial
        self.use_lxml = use_lxml
        self.try_ = 0
        self.proxies_list = pd.read_excel('instant_proxies.xlsx')['InstantProxies'].loc[21:35].tolist()
        
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
                    export_csv(error,'current_terra_error.csv')
                    raise Error502
                elif resp.status_code == 500:
                    error = {'error 500'}
                    export_csv(error,'current_terra_error.csv')
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


class MainContent:
    
    def __init__(self,url):
        self.url = url
        self.content = Call(self.url).call()
        self.name_ = '-'
        self.price_ = '-'
        self.location_ = '-'
        self.by_ = '-'
        self.link_ = '-'
        self.id_ = '-'
    
    def run_all(self,i):
        self.name(i)
        self.price(i)
        self.location(i)
        self.by(i)
        self.get_id()
        self.main_content = {'id':self.id_,'name':self.name_,'price':self.price_,'location':self.location_,
               'by':self.by_, 'link':self.link_}
        return {'id':self.id_,'name':self.name_,'price':self.price_,'distic-province':self.location_,
               'by':self.by_, 'link':self.link_}
    
    def get_data(self,i):
        self.run_all(i)
        x = self.main_content
        sub = SubContent(self.link_)
        sub.cont1_list_()
        sub.cont2_list_()
        sub.cont1_list
        sub.cont2_list
        y = sub.lazy_get_all()
        return {**x, **y}
    
    def name(self,i):
        name = self.content.xpath('//*[@id="card-col-list"]/div[4]/div[{}]/div/div/div[2]/div/h5/a'.format(i))
        try:
            self.name_ = name[0].text_content()
            self.link_ = 'https://www.baania.com' + name[0].get('href')
        except IndexError:
            print('no data at content {}'.format(i))
            raise NoContent('No content at content {}'.format(i))
    
    def get_id(self):
        self.id_ = self.link_[self.link_.find('-project')+9:]
    
    def price(self,i):
        price = self.content.xpath('//*[@id="card-col-list"]/div[4]/div[{}]/div/div/div[2]/div/div[2]/div'.format(i))
        try:
            self.price_ = price[0].text_content()
        except IndexError:
            raise NoContent('No content at content {}'.format(i))
    
    def location(self,i):
        location = self.content.xpath('//*[@id="card-col-list"]/div[4]/div[{}]/div/div/div[2]/div/div[3]/div[1]/small'.format(i))
        try:
            self.location_ = location[0].text_content()
        except IndexError:
            raise NoContent('No content at content {}'.format(i))
    
    def by(self,i):
        by = self.content.xpath('//*[@id="card-col-list"]/div[4]/div[{}]/div/div/div[2]/div/div[3]/div[2]/small'.format(i))
        try:
            self.by_ = by[0].text_content()
        except IndexError:
            raise NoContent('No content at content {}'.format(i))

class SubContent:
    
    def __init__(self,url):
        self.soup = Call(url=url,use_lxml=False).call()
        self.address = '-'
        self.cont1_list = []
        self.cont2_list = []
        self.dict_ = {'ประเภทอสังหาฯ':'-','สถานะ':'-','จำนวนแบบบ้าน':'-','ชั้น':'-','ที่จอดรถ':'-'
                      ,'เนื้อที่โครงการ':'-', 'จำนวนยูนิต':'-', 'ราคาสูงสุด':'-','โครงการเริ่มสร้าง':'-',
                     'พื้นที่ส่วนกลาง':'-'}
    
    def cont1_list_(self):
        cont1_list = []
        text_ = self.soup.find_all('div',{"class": "_3ICs5greeDOgzoXWexpeJB row"})
        for i in range(len(text_)):
            cont1_list.append(text_[i].get_text())
        self.cont1_list = cont1_list
    
    def cont2_list_(self):
        cont2_list = []
        text_ = self.soup.find_all('div',{"class": "_1aQRMW06zX7OWOEcH-wvcw row"})
        for i in range(len(text_)):
            cont2_list.append(text_[i].get_text())
        self.cont2_list = cont2_list
        
    def facility(self):
        self.facility_ = []
        x = self.soup.find_all('div', {'class' : 'h5 mb-0 d-md-inline-block _2RcJy3hWHnaoRPcjlBIRuz'})
        if len(x) > 0:
            for i in range(len(x)):
                self.facility_.append(x[i].get_text())
        
    
    def lazy_get_all(self):
        for string,variable in self.dict_.items():
            s_list = [s for s in self.cont1_list if string in s]
            if len(s_list) > 0:
                x = s_list[0]
                self.dict_[string] = x[len(string):]
        
        s_list = [s for s in self.cont2_list if 'ที่อยู่' in s]
        x = s_list[0]
        self.dict_['ที่อยู่'] = x[len('ที่อยู่'):]
        self.facility()
        self.dict_['สาธารณูปโภค'] = self.facility_
        return self.dict_

class LazyScrape:
    
    def __init__(self,filename='test.csv',start_page = 1,final_page = 99999,type_ = 'ทั้งหมด'):
        self.filename = filename
        self.start_page = start_page
        self.final_page = final_page
        self.type_ = type_
        self.type_dict_ = {'ทั้งหมด':'1,2,3','บ้าน':'1','คอนโด':'2','โฮมทาวน์':'3'}
        self.error502 = {}
        self.nocontent = {}
        self.IndexError = {}
    
    def scrape_csv(self):
        count=0
        for data in self.gen_cont():
            if count == 0:
                export_csv(data,self.filename,'w')
                count+=1
            else:
                export_csv(data,self.filename,'a')
                
    def append_csv(self):
        for data in self.gen_cont():
            export_csv(data,self.filename,'a')
    
    def gen_url(self):
        for i  in range(self.start_page,self.final_page):
            self.url = 'https://www.baania.com/th/s/ทั้งหมด/project?mapMove=true&page={}&propertyType={}&'.format(i,self.type_dict_[self.type_])
            yield self.url
    
    def gen_cont(self):
        count = 0
        for url in self.gen_url():
            m = MainContent(url=url)
            for i in range(1,49):
                if count > 40:
                    raise Exception('might finished')
                try:
                    yield m.get_data(i)
                except Error502:
                    self.error502[self.url] = i
                    continue
                except NoContent:
                    self.nocontent[self.url] = i
                    count = count+1
                    print(count)
                except IndexError:
                    self.IndexError[self.url] = i

def export_csv(data,filename,mode):
    with open(filename,mode=mode,encoding='utf8') as csvfile:
        fieldnames = list(data.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        writer.writerow(data)

if __name__ == "__main__":
    lzs = LazyScrape('baania_hometown.csv', start_page=95, final_page=108, type_='โฮมทาวน์')
    lzs.append_csv() ## dont forget to change to append



