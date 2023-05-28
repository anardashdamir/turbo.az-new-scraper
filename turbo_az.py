import pandas as pd 
import numpy as np 
import requests 
from bs4 import BeautifulSoup
import threading
import time
from datetime import datetime


print('Başladı')

exc_start = time.time()
page_count = 416
urls = list()


for page in range(1, page_count+1):
    url = f'https://turbo.az/autos?page={page}'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    items = soup.find_all('a', class_='products-i__link')
    urls.append([a_tag.get('href') for a_tag in items]) 

   
urls = np.concatenate(urls, axis=None)
urls = np.array(['https://turbo.az' + url for url in urls])
urls = np.unique(urls)

print(f'Url Handling: {time.time() - exc_start}')



data_list = list()
errors = list()

def scrape(url):
    
    """
    Scrapes data from a webpage based on the provided URL.

    Args:
        url (str): The URL of the webpage to scrape.
    """
    
    data = pd.DataFrame()
    
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    
    owner = soup.find('div', 'product-owner__info')
    
    try:
        data['Satici'] = [owner.find('div', class_='product-owner__info-name').text]
        data['Avtosalon'] = 0
    except AttributeError:
        data['Satici'] = [soup.find('div', class_='product-shop__owner-name').text]
        data['Avtosalon'] = 1
    finally: errors.append(('Satici', url))
        
        
        
    try:
        data['Telefonlar'] = ' && '.join([i.text.strip() for i in soup.find_all('a', class_='product-phones__list-i')])
    except: errors.append(('Telefon', url))
    
    
    try:
        side_box = soup.find('div', class_='product-sidebar__box')
        full_price = side_box.find('div', class_='product-price__i').text.split()
        data['Qiymet'], data['Valyuta'] = [int(''.join(full_price[:len(full_price) - 1]))], [full_price[-1]]
    except: errors.append(('Qiymet', url))
    
    
    
    try:
        props_container = soup.find('div', class_='product-properties')
        props = props_container.find_all('div', class_='product-properties__i')
        keys = [i.find('label').text.strip() for i in props]
        vals = [i.find('span').text.strip() for i in props]

        
        for key, val in zip(keys, vals):
            data[key] = [val]
    except: errors.append(('Props', url))
        
    
    
    try:
        stats = soup.find('ul', class_='product-statistics').find_all('li')
        for i in stats:
            txt = i.text 
            key = txt.split(':')[0]
            val = txt.split(':')[1]
            data[key] = [val]
    except: errors.append(('Stats', url))
    
    try:
        extra = soup.find('ul', class_='product-extras').find_all('li')
        data['Extra'] = [', '.join([i.text.strip() for i in extra])]
    except: errors.append(('Extra', url))
    
    try:
        data['Etrafli'] = ' '.join([i.text.strip().replace('\n', ' ') for i in soup.find('div',
                                            class_='product-description__content js-description-content').find_all('p')])
    except: errors.append(('Etrafli', url))
    
    data['Url'] = [url]
    
    data_list.append(data)

    
    
url_count = len(urls)
thread_count = 35

arr = [*range(0, url_count, thread_count)]
length = [*range(url_count)]
i = 0

while True:
    all_threads = []
    try:
        for e in length[arr[i]:arr[i + 1]]:
            thread = threading.Thread(target=scrape, args=(urls[e],))
            all_threads.append(thread)

        for thread in all_threads:
            thread.start()

        for thread in all_threads:
            thread.join()

        i += 1
    except:
        for e in length[arr[i]:]:
            thread = threading.Thread(target=scrape, args=(urls[e],))
            all_threads.append(thread)

        for thread in all_threads:
            thread.start()

        for thread in all_threads:
            thread.join()
        break  
    
    
    
main_data = pd.concat(data_list, ignore_index=True)
main_data.to_csv(f'{datetime.now().strftime("%b-%d-%Y %H-%M-%S")}.csv',
                 encoding='utf_8_sig',
                 index=False)


print(*errors, end='\n')
print(f'Execution time: {time.time() - exc_start}')
    
      

