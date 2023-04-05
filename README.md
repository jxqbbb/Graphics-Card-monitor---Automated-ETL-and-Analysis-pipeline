# Graphics processing unit price and specification monitor - Automated ETL and Analysis pipeline
## Project's overview
This Python and SQL project scrapes data on GPU sales offers posted on Amazon, performs data cleaning, and loads the data into a database. Pipeline's performance is documented as a report, which is automatically emailed to the user. The project also includes analysis and visualization to examine trends in GPU specifications and pricing. Whole process is automated and to use it all you need is running "main.py" with chosen parameters. To ensure that the scripts are reusable and accessible to all users, the project uses object-oriented programming and includes extensive validation measures. The project is intended for regular data collection, such as once a month, to track changes in GPU pricing and specifications over time.
## Pipeline Step-by-Step
### ETL process (etl_process.py)
- Collecting data from Amazon 🛒
- Cleaning the data 🧹
- Loading it into SQL database 💾
- Sending a summary report to email 📩
### Analysis process (analysis_process.py)
- Fetching data from database 💾
- Performing summary statistics and analysis of the data 🔍
- Visualizing insights of the data 📊
### Automatically collect and analyse data regularly (main.py (wraps functinality of etl_process.py andanalysis_process.py ) + task scheduler)
- Repeat the process automatically for example by using Windows Task Scheduler to run "main.py" each month (in my case) 📅
## Tools used:
Programming language:
- Python:
  - IDE:
    - PyCharm
    - Jupyter Notebook
  - Environment:
    - python 3.10.4
    - external packages:
      - collecting data:
        - beautifulsoup4 4.11.1
        - requests 2.28.1
      - data wrangling:
        - pandas 1.4.4
        - numpy 1.21.5
      - working with database:
        - sqlalchemy 1.4.39
        - cryptography 37.0.1
      - visualization:
        - matplotlib 3.5.2
        - seaborn 0.11.2
       
Data storage
- SQL:
  - RDBMS
    - MySQL
- File:
  - CSV file format  

Sources:
- Data source:
  - [Amazon   ](https://www.amazon.com/)
## Analysis part results
![p1](https://user-images.githubusercontent.com/121947030/230118328-3e2bdd04-6214-4778-b661-7c21afd0396c.png)
![p2](https://user-images.githubusercontent.com/121947030/230118372-c6f8a269-efd9-44f8-8e17-6eeb784f2bf9.png)

Note that time series analysis cannot be performed yet due to the lack of data. However, it will make sense to examine the changes in prices and specifications when data is collected from different time periods later on.
## Other project snippets
### Example email reports:
![example_fail_email](https://user-images.githubusercontent.com/121947030/229369029-926ac054-c852-447f-bce4-cf07eb6de034.png)

![example_succes_email](https://user-images.githubusercontent.com/121947030/229369034-666df715-6d9a-43c1-b677-53c4a5283ddc.png)

### Example data stored in database:
![example_data_stored_in_database](https://user-images.githubusercontent.com/121947030/229369072-909805e8-f538-4534-9910-efc4d8608325.png)

## My thoughts about potential improvements
- Hosting this whole project on some cloud computing service like AWS
  then pipeline could be run scheduled using tool like AWS Lambda and data be stored in 
  cloud based database, advantage of such approach is fact that we don't need turn on our machine
  to enable process to be ran
- Changing dashboard tool, honestly matplotlib and seaborn python packages are not best tools
  for such things, I would consider using something like Tabelau or Power BI
- Some intersting idea is also developing website which would automatically update dashboard when script is ran  
- Using multithreading to speed up data collecting and even collect more data
- Implement rotating proxies or IP addresses not to be detected as a bot or web scraper
  
  

