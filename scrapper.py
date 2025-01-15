from bs4 import BeautifulSoup
import os
import time
import certifi
import urllib3
import re
import json

class GetData:
    def __fetch_url(self, base_link, retries=3, delay=5):
        http = urllib3.PoolManager(
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )
        for i in range(retries):
            try:
                response = http.request('GET', base_link)
                response.raise_for_status()
                return response
            except Exception as e:
                print(f'Attempt {i+1} failed: {e}')
                if i < retries - 1:
                    time.sleep(delay)
                else:
                    raise

    def __init__(self, base_link, force=False):
        if os.path.exists('scraped_page/web_data.html') and not force:
            with open('scraped_page/web_data.html', 'r') as file:
                self.soup_obj = BeautifulSoup(file.read(), 'html.parser')
        else:
            response = self.__fetch_url(base_link)
            self.soup_obj = BeautifulSoup(response.data, 'html.parser')
            # Backup a save of the scrapped web data in case of blocking
            if not os.path.exists('scraped_page'):
                os.makedirs('scraped_page')
            with open('scraped_page/web_data.html', 'w+') as file:
                file.write(str(self.soup_obj))

    def download(self, start_year=None):
        def download_file(itr_dict, name):
            nonlocal start_year
            nonlocal end_year
            try:
                for year_month, file_url in itr_dict.items():
                    if start_year is None or (int(year_month.split('-')[0]) >= start_year and int(year_month.split('-')[0]) <= end_year):
                        file_response = self.__fetch_url(file_url, retries=1)
                        if not os.path.exists('pdf_files'):
                            os.makedirs('pdf_files')
                        with open(f'pdf_files/{year_month}.pdf', 'w+') as file:
                            file.write(file_response.content)
                        print(f'Downloaded: {year_month}.pdf')
            except Exception as e:
                print(f'Error: {e}\nSaving links as .json files')
                if not os.path.exists('links_dump'):
                    os.makedirs('links_dump')
                with open(f'links_dump/{name}.json', 'w+') as file:
                    json.dump(itr_dict, file, indent=4)

        end_year = int(time.strftime("%Y")) - 1
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
        download_file(rabi_links, 'rabi_links')
        download_file(kharif_links, 'kharif_links')

if __name__ == '__main__':
    base_link = 'https://crs.agripunjab.gov.pk/reports'
    scraper = GetData(base_link)

    # Fetch data
    scraper.download()