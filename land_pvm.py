import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
import csv
import time
import os

class TrialExceed(Exception):
    pass

def export_csv(data,filename,mode):
        with open(filename,mode=mode,encoding='utf8') as csvfile:
            fieldnames = list(data.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if mode == 'w':
                writer.writeheader()
            writer.writerow(data) 


class LandScrape:
    
    field_name = ['จังหวัด','อำเภอ','ระวาง UTM',
 'แผ่นที่',
 'หน้าสำรวจ',
 'เลขที่ดิน',
 'เลขที่โฉนด',
 'ตำบล',
 'ราคาประเมิน (บาท ต่อ ตร.วา)']
    
    a_list = ['ระวาง UTM',
 'แผ่นที่',
 'หน้าสำรวจ',
 'เลขที่ดิน',
 'เลขที่โฉนด',
 'ตำบล',
 'ราคาประเมิน (บาท ต่อ ตร.วา)']
    
    def __init__(self,selChangwat,chanode_no,proxies_list):
        self.selChangwat = selChangwat
        self.chanode_no = chanode_no
        self.proxies_list = proxies_list
    
    def make_requests(self,page):
        while True:
            proxie = instant_proxies['proxie'][np.random.randint(len(instant_proxies))]
            try:
                self.resp = requests.post('http://property.treasury.go.th/pvmwebsite/search_data/s_land1_result.asp',
                                    data = {'chanode_no':self.chanode_no, 'survey_no':'','selChangwat':self.selChangwat,
                                           'page':page},
                                     proxies = {'http':'http://'+proxie, 'https':'https://'+proxie},
                                     timeout = 30)
                if self.resp.status_code == 200:
                    break
                else:
                    export_csv({'error':'status_code_'+str(self.resp.status_code)},'requests_error.csv','a')
                    time.sleep(2)
                    continue
            except requests.exceptions.ConnectionError:
                time.sleep(np.random.randint(1,10))
                export_csv({'error':'Connection_error'},'requests_error.csv','a')
                continue
            except requests.exceptions.ProxyError:
                time.sleep(np.random.randint(1,10))
                export_csv({'error':'ProxyError'},'requests_error.csv','a')
                continue
            except requests.exceptions.Timeout:
                time.sleep(np.random.randint(1,10))
                export_csv({'error':'Timeout'},'requests_error.csv','a')
                continue
                
    def get_data(self):
        for page in range(1,100):
            self.data_ = {}
            self.make_requests(page)
            soup = BeautifulSoup(self.resp.content.decode(self.resp.apparent_encoding), 'html.parser')
            df = pd.read_html(str(soup))
            if len(df) <= 1:
                if page == 1:
                    yield [self.data_,False]
                else:
                    break
            else:
                for i in range(1,len(df)):
                    self.data_['จังหวัด'] = df[i].loc[0].tolist()[0].split()[1]
                    self.data_['อำเภอ'] = df[i].loc[0].tolist()[0].split()[-1]
                    for key,item in zip(self.a_list,df[i].loc[2].tolist()[0:7]):
                        self.data_[key] = item
                    yield [self.data_,True]


if __name__ == "__main__":
    province = pd.read_csv('p_code.csv')
    instant_proxies = pd.read_excel('instant_proxies.xlsx',names=['proxie'])

    current_province_idx = 75
    current_chanode_no = 18378
    count = 1
    write = False
    round_ = 13
    for i in range(current_province_idx,77):
        p_code = province['P_CODE'][i]
        
        for chanode_no in range(current_chanode_no,1000000):
            time.sleep(np.random.randint(2))
            scraper = LandScrape(selChangwat=p_code,chanode_no=chanode_no,proxies_list=instant_proxies)
            count+=1
            
            for data,usable in scraper.get_data():
                print(i,chanode_no)
                if usable:
                    if write:
                        print('writing')
                        export_csv(data,'land_pvm_website.csv','w')
                        write = False
                    else:
                        print('adding')
                        export_csv(data,'land_pvm_website.csv','a')
                        if count > 50000:
                            count = 1
                            round_ += 1
                            write = True
                            client = storage.Client()
                            bucket = client.get_bucket('xxxxxxxxxxxx')
                            path = 'xxxxxxxxxxxxx'
                            filename = f'land_pvm_website_{round_}.csv'
                            blob = bucket.blob(os.path.join(path, filename))
                            local_filename = 'land_pvm_website.csv'
                            blob.upload_from_filename(local_filename)
                            time.sleep(1)
            if not usable:
                current_chanode_no=1
                break