from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from datetime import date
import random
import numpy as np
from sqlalchemy import create_engine
import os
from email.message import EmailMessage
import ssl
import smtplib

"""""Monitoring graphics processing unit prices by web scraping Amazon sale offers and fetching data about individual CPUs,
such as model name, price in USD, brand, graphics RAM size in gigabytes, GPU clock speed in megahertz, 
and assigning a date to the entire scrapped data to indicate the time period from which the data was collected, all of these
information is later automatically cleaned and  loaded to database for further analysis."""

# AmazonScrapeGPU class is Extract, Transform and Load part of the project

class AmazonScrapeGPU():
    # class instance parameters:
    # number_of_pages - define how many amazon pages to scrape -  40 by default
    # starting_page - define number of page to start scraping - 1 by default
    # headers - define http get request information about client, helpful in avoiding getting blocked for webscraping
    # - by default it is "User-Agent" based Samsung Galaxy S10 and other ordinary paremeters
    # base_url - amazon url to get computer graphics cards in "Computers & Accessories" section
    # filtered to get only factory new, unused GPU - by defult provided valid, working url
    # email and email_pass - variables needed to use Google's gmail account keep track of any issues and stay informed,
    # it is necessary to provide both the email and password associated with the email account.
    # this will enable monitoring of the script to ensure that everything is running smoothly and to quickly

    # database parameters:
    # in my case MySQL database is used and I retrive my login and password from windows environment variables
    # generally all default parameters are based on my needs
    # data - pandas DataFrame retrived from get_gpu_data function
    # database_user - RDBMS instance user or login
    # database_password - RDBMS instance password
    # database - name of used database
    # table - table of chosen database where data should be stored
    # host - host of the database
    # engine - optional parameter for non MySQL users, valid sqlalchemy create_enginge string should be passed

    def __init__(self,number_of_pages=40,starting_page=1,headers="unchanged",base_url="unchanged",
                email = os.environ.get("email"),email_pass=os.environ.get("email_pass")
                 ,database_user=os.environ.get("DB_USER"), database_password=os.environ.get("DB_PASS"),
                 database="gpu_monitoring", table="gpu_info", host="localhost", engine_str=None
                 ):

        #"the actual values of 'headers' and 'base_url' are not assigned as default parameters only for readability"
        #"these values are quite long"

        # if new "headers" paremeter wasn't given use the provided one
        if headers == "unchanged":
            self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",\
                            "Accept-Encoding":"gzip, deflate",\
                            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1",\
                            "Connection":"close",\
                            "Upgrade-Insecure-Requests":"1"}
        else:
            self.headers = headers

        # used to rotate user agent to avoid getting blocked by Amazon
        self.user_agents_list = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:111.0) Gecko/20100101 Firefox/111.0",
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.54",
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"]

        # if new "base_url" wasn't given use provided one
        if base_url == "unchanged":
            self.base_url = f"https://www.amazon.com/s?k=computer+graphics+cards&i=electronics&rh=n%3A172282%2Cp_n_condition-type%3A2224371011&dc&crid=1YK0ZOC8JKTBN&qid=1679749316&sprefix=%2Celectronics%2C165&ref=sr_pg_69"
        else:
            self.base_url = base_url


        self.number_of_pages = number_of_pages
        self.starting_page = starting_page

        #get current date
        self.date = date.today().strftime('%Y-%m-%d')

        # DataFrame which is the main storage of the scraped data before it will be send to database
        self.data_frame = pd.DataFrame(columns=["model","price_USD","brand","ram_GB","gpu_clock_speed_MHz","date"])

        # variables needed for sending report when script is finished
        self.email = email
        self.email_pass = email_pass
        # content of report itself will be upadated during execution of the script
        self.email_message = f"Data from {self.date}\n"

        # database connection parameters
        self.engine_str = engine_str
        self.host = host
        self.table = table
        self.database = database
        self.database_password = database_password
        self.database_user = database_user


    # --------------------------THIS IS "EXTRACT" PART OF THE PROJECT--------------------------

    # scraping individual sale offer
    # double underscore indicates that this should be private method
    def __get_gpu_info(self,url):
        print("get gpu info")
        #making random break in running script to avoid being detected as a scraper by the website
        time.sleep(random.randint(2, 10))

        # the purpose of "try" block here is to avoid crashing whole script if any single scraping proccess fails
        try:
            # request sale offer page with previousy specified headers
            r = requests.get(url,headers=self.headers)

        # if connecting to page failed don't stop the script but update email message
        # to be informed that something went wrong at some point
        except requests.exceptions.ConnectionError as connection_error:
            self.email_message+= f'Get GPU info:{url} connection error occurred: {connection_error}"\n'
        except requests.exceptions.Timeout as timeout_error:
            self.email_message+= f'Get GPU info:{url} timeout error occurred: {timeout_error}\n'
        except requests.exceptions.RequestException as request_error:
            self.email_message+= f'Get GPU info:{url} an error occurred: {request_error}\n'
        else:
            # check if website's response is ok
            if r.status_code < 300 and r.status_code > 100:

                # load html content of the page
                soup = BeautifulSoup(r.text,'lxml')

                # fetch information about price and check if such information was found
                price = soup.find("span",class_='a-price-whole')
                if price:
                    price = price.text
                else:
                    price = "unknown"

                # get specification setction
                product_info = soup.find("table",class_='a-normal a-spacing-micro')
                # check if it was found
                if product_info:


                    # get information about model, brand, RAM and GPU clock speed
                    model_info = product_info.find("tr",class_='a-spacing-small po-graphics_coprocessor')
                    brand_info = product_info.find("tr",class_='a-spacing-small po-brand')
                    ram_info = product_info.find("tr",class_='a-spacing-small po-graphics_ram.size')
                    gpu_clock_speed_info = product_info.find("tr",class_='a-spacing-small po-gpu_clock_speed')


                    # setting all information to 'unknown'; it will change depending on whether or not such information is found
                    model = "unknown"
                    brand = "unknown"
                    ram = "unknown"
                    gpu_clock_speed = "unknown"

                    # check if individual informations were found and if they were assign them
                    if model_info:
                        model_info = model_info.find_all("td")
                        if model_info[0].text.strip() == "Graphics Coprocessor":
                            model = model_info[1].text

                    if brand_info:
                        brand_info = brand_info.find_all("td")
                        if brand_info[0].text.strip() == "Brand":
                            brand = brand_info[1].text

                    if ram_info:
                        ram_info = ram_info.find_all("td")
                        if ram_info[0].text.strip() == "Graphics Ram Size":
                            ram = ram_info[1].text

                    if gpu_clock_speed_info:
                        gpu_clock_speed_info = gpu_clock_speed_info.find_all("td")
                        if gpu_clock_speed_info[0].text.strip() == "GPU Clock Speed":
                            gpu_clock_speed = gpu_clock_speed_info[1].text

                    # append gathered data to main storage DataFrame
                    self.data_frame.loc[len(self.data_frame.index)] = [model,price,brand,ram,gpu_clock_speed,self.date]

            # in case if error 401 or 403 occured script will be stopped, it means that probably
            # Amazon blocked the script and there is no point in further scraping
            # other status codes are not investigated becasue they might not necessarly make further scraping impossible
            # but if they do other function will break code later on
            elif r.status_code == 401:
                self.email_message+= f'Get GPU info:{url} access blocked by website (401)\n'
                raise requests.exceptions.HTTPError('401 Unauthorized')
            elif r.status_code == 403:
                self.email_message+= f'Get GPU info:{url} access blocked by website (403)\n'
                raise requests.exceptions.HTTPError('401 Unauthorized')


    #go through all GPU listing pages, from "starting_page" parameter through "number_of_pages" next pages
    # double underscore indicates that this should be private method
    def __iterate_pages(self):
        #get page indices defined by "starintg_page" and "number_of_pages"
        for x in range(self.starting_page,self.starting_page + self.number_of_pages):
            print("page ",x)
            # current page index
            current_page=x
            # concat "base_url" which directs to GPU sales listing page and page index to request another page
            page_link = f"{self.base_url}&page={current_page}"
            # use try statement to figure out what error might have occured to be informed in email message
            # and not to crash whole script when error occurs just use data that was able to be scrapped
            try:
                r = requests.get(page_link,headers=self.headers)
                r.raise_for_status()
                # sleep random time interval to not get blocked by website as bot
                time.sleep(random.randint(5,8))

            # if connecting to main page failed stop the script it might suggest that we reached last page, or url is invalid
            # or connection was blocked by amazon, either way there so point to continnue accessing another page

            except requests.exceptions.ConnectionError as connection_error:
                self.email_message+= f'Iterate pages: at page {current_page} connection error occurred: {connection_error}"\n'
                break
            except requests.exceptions.Timeout as timeout_error:
                self.email_message+= f'Iterate pages: at page {current_page} timeout error occurred: {timeout_error}\n'
                break
            except requests.exceptions.RequestException as request_error:
                self.email_message+= f'Iterate pages: at page {current_page} an error occurred: {request_error}\n'
                break

            # conntinue if request didn't raise error
            # information about error which occured is attached to email report
            else:
                # check if website's response is ok
                if r.status_code < 300 and r.status_code > 100:
                    # load html content of the page
                    soup = BeautifulSoup(r.text,"lxml")

                    # get list of GPU sales
                    offers_list = soup.find("div",class_="s-main-slot s-result-list s-search-results sg-row")

                    # check if it was found
                    if offers_list:
                        # store every offer in list
                        offers_list = offers_list.find_all("div",class_="sg-col-20-of-24 s-result-item s-asin sg-col-0-of-12 sg-col-16-of-20 sg-col s-widget-spacing-small sg-col-12-of-16")
                        # fetch url to each offer
                        for offer in offers_list:
                            # get url
                            offer_link = offer.find("a")
                            # check if it was found
                            if offer_link:
                                # use that url to fetch data about given graphics card
                                try:
                                    self.__get_gpu_info(f'https://amazon.com{offer_link["href"]}')

                                # if error 401 or 403 o
                                except requests.exceptions.HTTPError:
                                    break
                # if status code is not 200 it might suggest that we reached last page, or url is invalid
                # or connection was blocked by amazon, either way there so point to continnue accessing another page
                # information about negative response which occured is attached to email report
                elif r.status_code == 400:
                    self.email_message+= f'Iterate pages:{current_page} invalid request\n'
                    break
                elif r.status_code == 401:
                    self.email_message+= f'Iterate pages:{current_page} access blocked by website (401)\n'
                    break
                elif r.status_code == 403:
                    self.email_message+= f'Iterate pages:{current_page} access blocked by website (403)\n'
                    break
                elif r.status_code == 404:
                    self.email_message+= f'Iterate pages:{current_page} page not found\n'
                    break
                elif r.status_code == 500:
                    self.email_message+= f'Iterate pages:{current_page} internal server error\n'
                    break
                else:
                    self.email_message+= f'Iterate pages:{current_page} HTTP error occurred ({r.status_code})\n'
                    break

            # rotate user agent and the end of iteration to prevent website from blocking connection
            self.headers["User-Agent"] = self.user_agents_list[current_page%len(self.user_agents_list)]



    # --------------------------THIS IN "TRANSFORM" PART OF THE PROJECT--------------------------

    # clean scraped data
    # price, size of ram memory and GPU clock speed are all of string type
    # it is more convenient for future analysis to change it into numeric values
    # double underscore indicates that this should be private method
    def __prepare_data(self):
        # check if script was able to scrape anything
        if len(self.data_frame) > 0:

            print("cleaning")
            # GPU clock speed has "GHz"(gigahertz) or "MHz"(megahertz) postfix
            # these postfix is deleted and numeric value is returned as speed in megahertz
            def clock_speed_mhz(value):
                # make whole string lower letter for convenience of working with it
                value = value.lower()

                # check wether that string contains "ghz" or "mhz" postfix and based on that modify and return value

                # if given value is in gigahertz delete postfix and transform value to megahertz
                if "ghz" in value:
                    value = float(value.replace("ghz",""))
                    value = value*1000
                    return value
                # else just delete "mhz" postfix and return numeric value
                elif "mhz" in value:
                    return float(value.replace("mhz",""))
                else:
                    return value

            # get scraped data
            # copy of original data will be modified, in case if something goes wrong there is a backup
            df = self.data_frame.copy()

            # fetched price has comma at the end and and delimiter between number, it will be removed
            df["price_USD"] = df["price_USD"].str.replace(".","",regex=False).str.replace(",","",regex=False)
            # change "unkown" values to NaN values for convenience in analysis
            df["price_USD"].replace({"unknown":np.nan},inplace=True)
            # convert price to float for convenience in analysis
            df["price_USD"] = df["price_USD"].astype("float64")

            # remove gigabytes postfix from RAM size
            df["ram_GB"] = df["ram_GB"].str.replace("GB","",regex=False)
            # change "unkown" values to NaN values for convenience in analysis
            df["ram_GB"].replace({"unknown":np.nan},inplace=True)
            # convert RAM gigaytes size to float for convenience in analysis
            df["ram_GB"] = df["ram_GB"].astype("float64")

            # apply previously created function to GPU clock speed in MHz
            df["gpu_clock_speed_MHz"] = df["gpu_clock_speed_MHz"].apply(clock_speed_mhz)
            # change "unkown" values to NaN values for convenience in analysis
            df["gpu_clock_speed_MHz"].replace({"unknown":np.nan},inplace=True)
            # makes sure pandas treats these values as float
            df["gpu_clock_speed_MHz"] = df["gpu_clock_speed_MHz"].astype("float64")

            # getting rid of whitespaces in string columns
            df["model"] = df["model"].str.strip()
            df["brand"] = df["brand"].str.strip()

            # return cleaned DataFrame
            return df



    # this function is used to send report when program is finished running or was interrupted
    def send_email(self):
        # content of the email, sender, reciver, subject and message
        em = EmailMessage()
        em["From"] = self.email
        em["To"] = self.email
        em["Subject"] = "Automatic ETL report (ScrapeAmazonGPU)"
        body = self.email_message
        em.set_content(body)

        # securing the connection
        context = ssl.create_default_context()

        # actually sending the email using "smtplib" liblary
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(self.email, self.email_pass)
            smtp.sendmail(self.email, self.email, em.as_string())



    # wrapping whole extract and transform process to one method
    def get_gpu_data(self):
        # first fetch GPU data from Amazon pages
        self.__iterate_pages()
        # clean that data but in case if something goes wrong return uncleaned data which is also good enough
        # data cleaning is in most cases manual job and it's hard to predict format of hundreds collected values
        # to automate that process but based on my research and tests this should work
        try:
            # create variable indicating if data was successfully cleaned or not
            # it will be need when insterting data to database
            self.is_data_cleaned = True
            return self.__prepare_data()
        except:
            self.is_data_cleaned = False
            # note that cleaning wasn't sucessful
            self.email_message += "Cleaning part of the script failed\n"
            return self.data_frame




    # --------------------------THIS IS "LOAD" PART OF THE PROJECT--------------------------

    # load collected data to database
    # in my case MySQL database is used and I retrive my login and password from windows environment variables
    # generally all default parameters are based on my needs
    # data - pandas DataFrame retrived from get_gpu_data function
    # database_user - RDBMS instance user or login
    # database_password - RDBMS instance password
    # database - name of used database
    # table - table of chosen database where data should be stored
    # host - host of the database
    # engine_str - optional parameter for non MySQL users, valid sqlalchemy create_enginge string should be passed
    def load_to_db(self):
        # using collected and prepared data
        data = self.get_gpu_data()
        # check if any data was collected
        if len(data) > 0:
            # try to connect to specified database
            try:
                # if engine parameter was not passed create engine based on parameters for MySQL
                if not self.engine_str:
                # connecting using sqlalchemy package
                    engine = create_engine(f'mysql+pymysql://{self.database_user}:{self.database_password}@{self.host}/{self.database}')
                else:
                    engine = create_engine(self.engine_str)
                # insert data into the table, if table doesn't exist it will be created otherwhise data will be appended
                # if data was cleaned successfully insert data into main table
                if self.is_data_cleaned:
                    data.to_sql(name=self.table,con=engine,if_exists="append",index=False)
                    self.email_message+= f"Successfully loaded {len(data)} rows  to database\n"
                    engine.dispose()
                # otherwise insert data into backup table and store data for later manual cleaning
                else:
                    data.to_sql(name=f'{self.table}_uncleaned', con=engine, if_exists="append", index=False)
                    self.email_message += f"Successfully loaded {len(data)} rows  to database (uncleaned table)\n"
                    engine.dispose()
                # in case database connection fails, data will be stored localy in csv file

            # catch any exception and save it's content to attach that information to email
            except Exception as e:
                self.email_message+= f"Data couldn't be loaded to database - {e}, it was saved to csv file\n"
                # path of the file will have the same name as table, no specific path is specified so it will be saved in the script's folder
                path = f"{self.table}.csv"
                # if file exists, append data
                if os.path.isfile(path):
                    data.to_csv(path,na_rep="NaN",mode="a",index=False)
                # if file doesn't exist, create it
                else:
                    data.to_csv(path,na_rep="NaN",mode="w",index=False)

        else:
            self.email_message+="No data was collected"
    # this function wraps whole automated ETL process and executes it
    # and sends email with results report
    def run_etl_pipeline(self):
        self.load_to_db()
        self.send_email()






