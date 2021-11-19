import requests
import random
import time
import lxml.html
import pandas as pd
from bs4 import BeautifulSoup
import csv
import numpy as np
from tqdm.notebook import tqdm

def export_csv(data,filename,mode):
    with open(filename,mode=mode,encoding='utf8') as csvfile:
        fieldnames = list(data.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        writer.writerow(data)

def make_requests(url, instant_proxie):
    while True:
        try:
            random_ = np.random.randint(50)
            proxie = instant_proxie['proxie'][random_]
            proxies = {'http':'http://'+proxie, 'https':'https://'+proxie}
            resp = requests.get(url, proxies=proxies, timeout = 15)
            if resp.status_code == 200:
                break
            else:
                export_csv(resp.status_code, 'error.csv','a')
                break
        except requests.exceptions.Timeout:
            continue
        except requests.exceptions.ProxyError:
            continue
    
    cont = lxml.html.fromstring(resp.content.decode('utf-8','ignore'))        
    return cont

def get_data(cont, mode = 'a'):
    table = cont.xpath('/html/body/div[3]/table')[0]
    address = np.nan
    geometry_list=np.nan
    data = {'รหัสเดิม':np.nan, 'ชื่อหน่วยงาน':np.nan, 'ที่อยู่':np.nan, 'พื้นที่':np.nan,
           'สังกัด':np.nan, 'จำนวนเตียงผู้ป่วย':np.nan, 'สถานะการเปิดบริการ':np.nan, 'ระดับสถานบริการ':np.nan,
           'พิกัดทางภูมิศาสตร์':np.nan, 'ขนาดหน่วยบริการ':np.nan, 'จำนวนบุคลากรผู้ให้บริการ':np.nan, 
           'แพทย์เฉพาะทาง':np.nan, 'เครื่องมือแพทย์':np.nan}
    for i in range(100):
        try:
            list_ = table[i].text_content().split()
            if len(list_) > 0:
                first_element = list_[0]
                for key, _ in data.items():
                    if first_element.find(key) > -1:
                        data[key] = table[i].text_content().split()[1:]
                        break
        except IndexError:
            break
    export_csv(data, 'health_layer_for_upgradeV2.csv', mode)

if __name__ == "__main__":

    instant_proxie = pd.read_excel('instant_proxie.xlsx')
    df = pd.read_csv('all_moph.csv')

    df = pd.DataFrame(df[df['type'].isin(['โรงพยาบาลชุมชน', 'โรงพยาบาล/ศูนย์บริการสาธารณสุข สาขา',
                                        'โรงพยาบาล นอก สป.สธ.','โรงพยาบาลเอกชน','โรงพยาบาล นอก สธ.',
                                        'โรงพยาบาลศูนย์','โรงพยาบาลทั่วไป']) == True]).reset_index(drop=True)

    for i in tqdm(range(len(df))):
        url = df['url'][i]
        cont = make_requests(url, instant_proxie)
        if i == 0:
            get_data(cont, 'w')
        else:
            get_data(cont, 'a')