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

def get_data(resp):
    data={'name':np.nan, 'address':np.nan}
    cont = lxml.html.fromstring(resp.content.decode('utf-8', 'ignore'))
    data['name'] = cont.xpath('//*[@id="page-content"]/article/header/div[2]/div/div/div/div[2]/div[1]/a')[0].text
    data['address'] = cont.xpath('//*[@id="page-content"]/article/header/div[2]/div/div/div/div[2]/small/span[1]')[0].text.strip()
    return data

if __name__ == "__main__":

    main_url_df = pd.DataFrame({'url':[]})
    resp=requests.get('https://www.hipflat.co.th/market/condo-bangkok-skik')
    for i in range(1,51):
        bkk = lxml.html.fromstring(resp.content.decode('utf-8','ignore'))
        sub_url = bkk.xpath(f'//*[@id="page-content"]/div/div[1]/div[2]/div/ul/li[{i}]/a')[0].get('href')
        sub_url = 'https://www.hipflat.co.th'+sub_url
        url_dict = {'url':sub_url}
        main_url_df = main_url_df.append(url_dict, ignore_index=True)

    resp=requests.get('https://www.hipflat.co.th/market/condo-pattaya-fykx')

    for i in range(1,11):
        if i != 9:
            pattaya = lxml.html.fromstring(resp.content.decode('utf-8','ignore'))
            sub_url = pattaya.xpath(f'//*[@id="page-content"]/div/div[1]/div[2]/div/ul/li[{i}]/a')[0].get('href')
            sub_url = 'https://www.hipflat.co.th'+sub_url
            url_dict = {'url':sub_url}
            main_url_df = main_url_df.append(url_dict, ignore_index=True)


    for i in range(len(main_url_df)):

        url = main_url_df['url'][i]
        resp = requests.get(url)
        cont = lxml.html.fromstring(resp.content.decode('utf-8', 'ignore'))
        N = len(cont.xpath('//*[@id="page-content"]/div/div[1]/div[2]/div/ul/li'))
        for j in range(1,N+1):
            
            print(i,j)
            
            condo_url = cont.xpath(f'//*[@id="page-content"]/div/div[1]/div[2]/div/ul/li[{j}]/a')[0].get('href')
            condo_url = 'https://www.hipflat.co.th'+condo_url
            resp = requests.get(condo_url)
            data = get_data(resp)
            if write == 1:
                export_csv(data, 'hipflat_condo.csv', 'w')
                write = 0
            else:
                export_csv(data, 'hipflat_condo.csv', 'a')