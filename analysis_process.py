import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os


# --------------------------THIS IS ANALYSIS PART OF THE PROJECT--------------------------

# first data needs to be collected from database
# generally all default parameters are based on my needs
# database_user - RDBMS instance user or login
# database_password - RDBMS instance password
# database - name of used database
# table - table of chosen database where data should be stored
# host - host of the database
# engine_str - optional parameter for non MySQL users, valid sqlalchemy create_enginge string should be passed


# next analysis is performed using pandas, matplotlib and seaborn

# to specify the names under which to save the analysis visualizations
# you can use the parameters plot_name1 and plot_name2
# these correspond to the two separate figures that will be generated as output
# if either parameter is set to "None", the corresponding file will not be saved and only the dashboard will be displayed
# it is recommended to use Jupyter Notebook for better experience
def gpu_analysis_dashboard(plot_name1=None,plot_name2=None,database_user=os.environ.get("DB_USER"), database_password=os.environ.get("DB_PASS"),
                           database="gpu_monitoring", table="gpu_info", host="localhost",engine_str=None):
    # separate function to collect data from database
    # double underscore indicates that it is a private function
    def __load_from_db():
        # connecting to database using previously set parameters
        # in this case the is no point in cathing any errors because if collecting data fails
        # there is no point to perform any analysis


        if not engine_str:
            engine = create_engine(f'mysql+pymysql://{database_user}:{database_password}@{host}/{database}')
        else:
            engine = create_engine(engine_str)
        connection = engine.connect()
        # load data using pandas and save it into variable
        data = pd.read_sql(f"{table}", con=connection,
                           columns=["model", "price_USD", "brand", "ram_GB", "gpu_clock_speed_MHz", "date"])
        # after everything is read close the connection to database
        connection.close()

        # return data for futher analysis
        return data

    # here the analysis begins, data is collected from database
    df = __load_from_db()

    # some visualization based on collected GPU data, there is no time series analysis yet due to lack of data from different
    # time peirods

    # building dashboard split into 2 matplotlib figures

    # first figure will contain 3 large plots that summarize sold GPUs on Amazon
    plt.style.use("dark_background")
    fig, ax = plt.subplots(3, 1, figsize=(10, 22))

    # data needed for first plot

    # avarage price for each model on sale
    price_by_model = df.groupby(["model"]).agg(price=("price_USD", "mean")).sort_values(by="price", ascending=False)
    price_by_model.reset_index(inplace=True)

    # first plot
    sns.barplot(data=price_by_model, x="price", y="model", orient='h', ax=ax[0])

    # some customization like setting descriptions of X and Y, title etc.
    ax[0].set_xlabel("US dollars price", fontsize=14)
    ax[0].set_ylabel("GPU model")
    # display actual values inside each bar
    ax[0].bar_label(ax[0].containers[0], label_type="center")
    ax[0].title.set_text("Avarage prices of GPU models")

    # data needed for second plot

    # aggregated data describing avarage GPU price by brand
    price_by_brand = df.groupby(["brand", "model"])["price_USD"].mean().reset_index().groupby("brand")[
        "price_USD"].mean().sort_values()

    # second plot
    brand_bars = sns.barplot(x=price_by_brand.index, y=price_by_brand.values, ax=ax[1])
    # display actual values on each bar
    brand_bars.bar_label(brand_bars.containers[0])

    # some customization
    ax[1].set_xticklabels(labels=price_by_brand.index, rotation=40)
    ax[1].set_xlabel('Brand', fontsize=14)
    ax[1].set_ylabel('Avarage price $', fontsize=16, rotation=0, labelpad=70)
    ax[1].title.set_text("Avarage GPU USD price by brand")

    # third plot, in this case there was no need to prepare data
    ax[2].pie(df["brand"].value_counts(), labels=df["brand"].value_counts().index, autopct='%1.1f%%',
              textprops={"color": "black"})
    # customization
    ax[2].set_ylabel("Which GPU brand is \n the most sold?", fontsize=16, rotation=0, labelpad=110)

    # save first part of the dasboard
    if plot_name1:
        plt.savefig(plot_name1,dpi=200,bbox_inches='tight')

    # second figure will contain 6 small plots that display some descriptive statistics and information about GPUs specification
    fig, axs = plt.subplots(3, 2, figsize=(10, 14))

    # data needed for first and second plot

    # calculating how many dollars have to be spent to get one gigabyte of RAM and how many dollars have to be spent
    # to get 100 MHz of clock speed for each GPU model

    # group data into models
    model_group = df.groupby("model")
    # calculate mean price for each model
    x = model_group["price_USD"].mean()
    # calculate mean amount of RAM for each GPU model
    y = model_group["ram_GB"].mean()
    # calculate mean amount of MHz for each GPU model
    z = model_group["gpu_clock_speed_MHz"].mean()

    # calculate value per dollars
    dollars_per_ram_gigabyte = (x / y)
    dollars_per_100_MHz = (x / z) * 100

    # first plot
    sns.barplot(x=dollars_per_ram_gigabyte.values, y=dollars_per_ram_gigabyte.index, ax=axs[0, 0])

    # customization
    axs[0, 0].set_title('US dollars per 1 RAM gigabyte')
    axs[0, 0].set_xlabel("US dollars", fontsize=10)
    axs[0, 0].set_ylabel("GPU model")

    # second plot
    sns.barplot(x=dollars_per_100_MHz.values, y=dollars_per_100_MHz.index, ax=axs[0, 1])

    # customization
    axs[0, 1].set_yticklabels([])
    axs[0, 1].set_title('US dollars per 100 GPU clock Mhz')
    axs[0, 1].set_xlabel('US dollars', fontsize=10)
    axs[0, 1].set_ylabel('')

    # third plot, no data transforming was needed
    sns.regplot(data=df, x="gpu_clock_speed_MHz", y="price_USD", ax=axs[1, 0])

    # customization
    axs[1, 0].set_title('Relation between price and clock speed')
    axs[1, 0].set_xlabel('GPU clock speed [MHz]')
    axs[1, 0].set_ylabel('US dollars', fontsize=14, rotation=0, labelpad=60)

    # data needed for fourth plot

    # binning GPU's clock speeds into groups
    df["MHz_group"] = pd.cut(df["gpu_clock_speed_MHz"],
                             bins=np.linspace(df["gpu_clock_speed_MHz"].min(), df["gpu_clock_speed_MHz"].max(), 5),
                             labels=["very low", "low", "moderate", "high"])

    # fourth plot
    sns.barplot(data=df, x="MHz_group", y="price_USD", ax=axs[1, 1])

    # customization
    axs[1, 1].set_title('Avarage prices in different clock speed categories')
    axs[1, 1].set_xlabel('Category')
    axs[1, 1].set_ylabel('')

    # fifth plot, no data transforming was needed
    sns.boxplot(data=df["price_USD"], ax=axs[2, 0])

    axs[2, 0].set_xticklabels(["Prices"])
    axs[2, 0].set_title('Price distribution in sales offers')
    axs[2, 0].set_xlabel('')
    axs[2, 0].set_ylabel('Count', fontsize=14, rotation=0, labelpad=70)

    # sixth plot, no data transforming was needed
    sns.histplot(data=df["price_USD"], bins=7, kde=True, ax=axs[2, 1])

    # customization
    axs[2, 1].set_title('Price distribution in sales offers')
    axs[2, 1].set_xlabel('Prices')
    axs[2, 1].set_ylabel('')

    # adjust the layout of the subplots
    fig.tight_layout()

    # save second part of the dashboard
    if plot_name2:
        plt.savefig(plot_name2,dpi=200,bbox_inches='tight')

    # display the plot
    plt.show()
 
# sample usage, saving plots as "test1" and "test2"
gpu_analysis_dashboard("test1","test2")
