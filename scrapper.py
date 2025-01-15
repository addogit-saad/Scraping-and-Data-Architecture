import requests
from bs4 import BeautifulSoup
import os
import time
import certifi
import urllib3
import re

class GetData:
    def __fetch_url(self, retries=3, delay=5):
        http = urllib3.PoolManager(
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )
        for i in range(retries):
            try:
                response = http.request('GET', self.base_link)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                print(f'Attempt {i+1} failed: {e}')
                if i < retries - 1:
                    time.sleep(delay)
                else:
                    raise

    def __init__(self, base_link, force=False):
        self.base_link = base_link
        if os.path.exists('scraped_page/web_data.html') and not force:
            with open('scraped_page/web_data.html', 'r') as file:
                self.soup_obj = BeautifulSoup(file.read(), 'html.parser')
        else:
            response = self.__fetch_url()
            self.soup_obj = BeautifulSoup(response.data, 'html.parser')
            # Backup a save of the scrapped web data in case of blocking
            if not os.path.exists('scraped_page'):
                os.makedirs('scraped_page')
            with open('scraped_page/web_data.html', 'w+') as file:
                file.write(str(self.soup_obj))

    def download(self, start_year=None):
        current_year = time.strftime("%Y")
        annual_books_section = self.soup_obj.find('table')
        kharif_links, rabi_links = {}, {}
        for link in annual_books_section.find_all('a', href=True):
            if re.match(r'https://crs-agripunjab.punjab.gov.pk/system/files/.*Kharif.*', link['href'], re.IGNORECASE):
                temp = link['href']
                match = re.search(r'(\d{4}-\d{2})', temp)
                year_month = match.group(1)
                kharif_links[year_month] = link['href']
            if re.match(r'https://crs-agripunjab.punjab.gov.pk/system/files/.*Rabi.*', link['href'], re.IGNORECASE):
                temp = link['href']
                match = re.search(r'(\d{4}-\d{2})', temp)
                year_month = match.group(1)
                rabi_links[year_month] = link['href']
        
        # Sort the dictionary
        rabi_links = dict(sorted(rabi_links.items(), key=lambda x: -int(x[0].split('-')[0])))
        kharif_links = dict(sorted(kharif_links.items(), key=lambda x: -int(x[0].split('-')[0])))
        
        # Download the files
        for year_month, file_url in rabi_links.items():
            if start_year is None or int(year_month.split('-')[0]) >= start_year:
                file_response = requests.get(file_url)
                file_response.raise_for_status()
                with open(f'{year_month}.pdf', 'w+') as file:
                    file.write(file_response.content)
                print(f'Downloaded: {year_month}.pdf')

if __name__ == '__main__':
    base_link = 'https://crs.agripunjab.gov.pk/reports'
    scraper = GetData(base_link)

    # Fetch data
    scraper.download()