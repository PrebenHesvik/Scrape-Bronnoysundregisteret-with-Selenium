import time
import random
import logging
import json
import pandas as pd
import numpy as np
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme

logging.basicConfig(
    filename='scrape-log.log',
    level=logging.INFO,
    filemode='w',
    )

console = Console()
custom_theme = Theme({'success': 'green', 'error': 'bold red'})


class Scraper:
    def __init__(
        self, 
        web_driver, 
        use_local_driver,
        path_local_chromedriver='webdrivers/chromedriver.exe',
        path_local_geckodriver='webdrivers/geckodriver.exe'
        ):
        
        self.use_local_driver = use_local_driver
        self.path_local_chromedriver = path_local_chromedriver
        self.path_local_geckodriver = path_local_geckodriver
        self.select_web_driver(web_driver)
        
    def select_web_driver(self, web_driver):
        if web_driver.lower() == 'chrome':
            if self.use_local_driver is True:
                self.driver = webdriver.Chrome(
                    executable_path=self.path_local_chromedriver
                )
            else:
                s = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=s)
        
        elif web_driver.lower == 'firefox':
            if self.use_local_driver is True:
                self.driver = webdriver.Firefox(
                    executable_path=self.path_local_geckodriver
                )
            else:
                s = Service(GeckoDriverManager().install())
                self.driver = webdriver.Firefox(service=s)
        else:
            raise ValueError('Can only select Chrome or Firefox webdriver')
            
    
    def check_if_xpath_exists(self, xpath, return_str=''):
        try:
            return self.driver.find_element(By.XPATH, xpath).text
        except NoSuchElementException:
            return return_str
        
class BrregScraper(Scraper):
    
    xpaths = {
        'front-page-search-box': '//div[@class="input-group"]/input[@placeholder="Søk på navn eller organisasjonsnummer"]',
        'front-page-button': '//*[@id="companySearch"]/div',
        'name': '//div[@id="pagecontent"]/div[3]/div[2]/p',
        'address': '//div[@id="pagecontent"]/div[5]/div[2]/p',
        'municipality': '//*[@id="pagecontent"]/div[6]/div[2]/p',
        'postal_address': '//*[@id="pagecontent"]/div[7]/div[2]/p',
        'company_not_found': '//*[@id="pagecontent"]/p',  
    }
    
    def __init__(
        self, 
        xpaths_to_exclude=None, 
        include_country_code=True,
        use_local_driver=False,
        web_driver='chrome',
        path_local_chromedriver='webdrivers/chromedriver.exe',
        path_local_geckodriver='webdrivers/geckodriver.exe'
    ):
        super().__init__(
            web_driver=web_driver, 
            use_local_driver=use_local_driver,
            path_local_chromedriver=path_local_chromedriver, 
            path_local_geckodriver=path_local_geckodriver, 
            
        )
        self.url = 'https://www.brreg.no/'
        self.include_country_code=include_country_code
        self.xpaths_to_exclude = xpaths_to_exclude
        self.error_log = {}
        self.data_log = {}
        self.searchable_xpaths()
        
    
    def searchable_xpaths(self):
        do_not_search = [
            'front-page-search-box', 
            'front-page-button', 
            'company_not_found'
        ]
        
        if self.xpaths_to_exclude is not None:
            do_not_search += self.xpaths_to_exclude
        self.xpaths_to_search = {
            k: v for k, v in self.xpaths.items() 
            if k not in do_not_search
        }
    
    
    def scrape_data(self, supplier_id, vat_num):
        # remove everything except numbers
        vat_str = ''.join(l for l in vat_num if l.isdigit())
        
        self.driver.get(self.url)
        self.driver.find_element(By.XPATH, self.xpaths.get('front-page-search-box')).send_keys(vat_str)
        self.driver.find_element(By.XPATH, self.xpaths.get('front-page-search-box')).send_keys(Keys.ENTER)
        
        
        if self.check_if_xpath_exists(self.xpaths.get('name')) == '':
            element = self.xpaths.get('company_not_found')
            msg = self.check_if_xpath_exists(element, return_str='Could not find error message')
            #msg = self.driver.find_element(By.XPATH, element).text
            console.print(f'[bold red]Warning:[/] [bold cyan]{supplier_id} {vat_num}[/]: {msg}')
            logging.info(f'\n \t{supplier_id}\n \t{msg}')
            self.error_log[supplier_id] = msg
            
        else:
            brreg_data = {
                k: self.check_if_xpath_exists(v, return_str=np.NaN) for k, v in 
                self.xpaths_to_search.items()
            }
            
            self.data_log[supplier_id] = brreg_data
            return {**{'supplier_id': supplier_id, 'vat_number': vat_num}, **brreg_data}
    
    
    def scrape(self, vat_numbers_dict):
        data_dict_lst = []
        for supplier_id, vat_num in vat_numbers_dict.items():
            data_dict = self.scrape_data(supplier_id, vat_num)
            if data_dict is not None:
                data_dict_lst.append(data_dict)
            time.sleep(random.randint(0, 3))
        self.driver.quit()
        return self.clean_and_store_data(data_dict_lst)
        
    
    def clean_and_store_data(self, data_dict_lst):
        
        with open('output/error_log.json', 'w', encoding='utf-8') as handle:
            json.dump(self.error_log, handle, ensure_ascii=False)
        
        df = pd.DataFrame.from_dict(data_dict_lst, orient='columns')
        
        if df.shape[0] > 0 and 'address' in self.xpaths_to_search.keys():
            df = df.pipe(self.clean_address)
        
        if df.shape[0] > 0 and 'postal_address' in self.xpaths_to_search.keys():
            df = df.pipe(self.clean_postal_address)
        
        writer = pd.ExcelWriter('output/scraped_data.xlsx')
        df.to_excel(writer, index=False)
        writer.save()
        
        return df

    
    @staticmethod
    def clean_address(df):
        ser = df.address.str.split('\n').str[-2:].str.join(' ')
        city = ser.str.split(' ').str[-1]
        postal_code = ser.str.split(' ').str[-2]
        road = ser.str.split(' ').str[:-2].str.join(' ')
        
        zipped = zip(
            ['street', 'postal_code', 'city'], 
            [road, postal_code, city]
        )

        for col, series in zipped:
            df[col] = series
            
        df.drop(columns=['address'], inplace=True)
        
        return df
    
    
    @staticmethod
    def clean_postal_address(df):
        ser = df.postal_address.str.split('\n')

        att = np.where(
            ser.str.len() == 3,
            ser.str[0],
            np.NaN
        )

        remaining_ser = ser.str[-2:].str.join(',').str.split(',', expand=True)
        postal_street = remaining_ser.pop(0)
        postal_postal_code = remaining_ser[1].str.split(' ').str[0]
        postal_city = remaining_ser[1].str.split(' ').str[1]

        col_names = ['postal_att', 'postal_street', 'postal_postal_code', 'postal_city']
        series = [pd.Series(att), postal_street, postal_postal_code, postal_city]
        
        zipped = zip(col_names, series)

        for col, series in zipped:
            df[col] = series
            
        df.drop(columns=['postal_address'], inplace=True)

        return df