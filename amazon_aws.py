import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime as dt
import os
import requests
import csv
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import numpy as np 
import pandas as pd
from dotenv import load_dotenv
import papermill as pm
import airflow.hooks.S3_hook
from airflow.operators.postgres_operator import PostgresOperator
import airflow.hooks.postgres_hook

dotenv_local_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)

# ----------------------------------------------------------------------------------------------------
# Setup DAG

default_args = {
	'owner':'amit',
	#'start_date': dt(2020,11,4,0),
	'start_date': dt.now(),
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
	'amazon_aws',
	default_args = default_args,
	description = 'amazon order history',
	#schedule_interval = timedelta(hours=1),
	catchup = False,
	max_active_runs = 1,
	)

# ----------------------------------------------------------------------------------------------------
# Get input csv stored on AWS S3

csv_input_path = '/Users/amit/Coding/Projects/AmazonOrderHistoryAirflowAWS/amazon_purchases.csv'

def get_amazon_purchases():
	"""
	Gets Amazon order history csv stored on AWS S3.
	"""
	url = "https://amazon-order-history.s3.amazonaws.com/amazon_purchases.csv"
	response = requests.get(url)
	#path = os.path.join(os.path.dirname(__file__),"../amazon_purchases.csv")
	path = csv_input_path
	with open(path, 'wb') as f:
		f.write(response.content)

t1 = PythonOperator(
	task_id = 'get_amazon_purchases',
	python_callable = get_amazon_purchases,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Read input csv into pandas, peform ETL, export final dataframe as output csv

csv_output_path = '/Users/amit/Coding/Projects/AmazonOrderHistoryAirflowAWS/amazon_purchases_2.csv'

def etl_csv():

	# Read csv
	df = pd.read_csv(csv_input_path, parse_dates=['Order Date', 'Shipment Date'])

	# Rename columns to remove spaces
	df.columns = df.columns.str.replace(' ', '')

	# Rename Unncessary columns
	df = df.rename(columns={'CarrierName&TrackingNumber':'Carrier', 'ItemSubtotalTax': 'Tax', 
		'ShipmentDate':'ShipDate'})

	# Drop Website column
	del df['Website']

	# Replace NaN
	df.Category.fillna('unknown', inplace = True)
	df.Condition.fillna('unknown', inplace = True)
	df.Carrier.fillna('unknown', inplace = True) 

	# Remove $ and , from price columns
	df['ListPricePerUnit'] = df['ListPricePerUnit'].str.replace('$','').str.replace(',','')
	df['PurchasePricePerUnit'] = df['PurchasePricePerUnit'].str.replace('$','').str.replace(',','')
	df['ItemSubtotal'] = df['ItemSubtotal'].str.replace('$','').str.replace(',','')
	df['Tax'] = df['Tax'].str.replace('$','').str.replace(',','')
	df['ItemTotal'] = df['ItemTotal'].str.replace('$','').str.replace(',','')

	# Convert price columns to float
	df['ListPricePerUnit'] = df['ListPricePerUnit'].astype(float)
	df['PurchasePricePerUnit'] = df['PurchasePricePerUnit'].astype(float)
	df['ItemSubtotal'] = df['ItemSubtotal'].astype(float)
	df['Tax'] = df['Tax'].astype(float)
	df['ItemTotal'] = df['ItemTotal'].astype(float)

	# Drop rows with zero prices
	df = df[df.ListPricePerUnit != 0]
	df = df[df.PurchasePricePerUnit != 0]
	df = df[df.ItemSubtotal != 0]
	df = df[df.ItemTotal != 0]

	# Extract year, month, & day and store them in columns in df_main
	df['OrderYear'] = df['OrderDate'].dt.year
	df['OrderMonth'] = df['OrderDate'].dt.month
	df['OrderDay'] = df['OrderDate'].dt.day
	df['OrderDayIndex'] = df['OrderDate'].dt.dayofweek
	df['OrderDayName'] = df['OrderDate'].dt.day_name()

	# Drop rows where year = 2020
	df = df.drop(df[df['OrderDate'].dt.year == 2020].index)

	# Combine carriers to eliminate repitition
	df['Carrier'] = df['Carrier'].replace('FEDEX', 'FedEx')
	df['Carrier'] = df['Carrier'].replace('SMARTPOST', 'FedEx SmartPost')
	df['Carrier'] = df['Carrier'].replace('Mail Innovations','UPS Mail Innovations')
	df['Carrier'] = df['Carrier'].replace('UPS MI','UPS Mail Innovations')
	df['Carrier'] = df['Carrier'].replace('US Postal Service','USPS')
	df['Carrier'] = df['Carrier'].replace('DHL Global Mail','DHL')
	df['Carrier'] = df['Carrier'].replace('US Postal Service','USPS')
	df['Carrier'] = df['Carrier'].replace('AMZN_US', 'AMZN')
	mail = ['USPS', 'UPS', 'UPS Mail Innovations', 'FedEx', 'FedEx SmartPost', 'DHL', 'AMZN']
	df.loc[~df.Carrier.isin(mail), 'Carrier'] = 'Other'

	# Combine categories
	df['Category'] = df['Category'].replace(['NOTEBOOK_COMPUTER','COMPUTER_DRIVE_OR_STORAGE','RAM_MEMORY','TABLET_COMPUTER','MONITOR','COMPUTER_COMPONENT', 'FLASH_MEMORY', 'SOFTWARE', 'INK_OR_TONER', 'COMPUTER_INPUT_DEVICE', 'CABLE_OR_ADAPTER', 'NETWORKING_DEVICE', 'KEYBOARDS', 'COMPUTER_ADD_ON', 'NETWORKING_ROUTER','MEMORY_READER','WIRELESS_ACCESSORY','SCANNER','PRINTER', 'ABIS_DOWNLOADABLE_SOFTWARE'],'COMPUTER')
	df['Category'] = df['Category'].replace(['HEADPHONES','SPEAKERS','BATTERY','MULTIFUNCTION_DEVICE','ELECTRONIC_CABLE','SURVEILANCE_SYSTEMS','SECURITY_CAMERA','WATCH','CONSUMER_ELECTRONICS','CE_ACCESSORY','ELECTRONIC_ADAPTER','ELECTRIC_FAN','CAMCORDER','HANDHELD_OR_PDA','TUNER','AMAZON_BOOK_READER','CELLULAR_PHONE','POWER_SUPPLIES_OR_PROTECTION','CAMERA_OTHER_ACCESSORIES','CHARGING_ADAPTER', 'ABIS_ELECTRONICS', 'SYSTEM_POWER_DEVICE'],'ELECTRONICS')
	df['Category'] = df['Category'].replace(['HAIR_STYLING_AGENT','PERSONAL_CARE_APPLIANCE','PROFESSIONAL_HEALTHCARE','HEALTH_PERSONAL_CARE','SHAMPOO','VITAMIN','ABIS_DRUGSTORE','BEAUTY', 'DIETARY_SUPPLEMENTS'],'HEALTH_BEAUTY')
	df['Category'] = df['Category'].replace(['KITCHEN','SEEDS_AND_PLANTS','HOME_LIGHTING_ACCESSORY','BOTTLE','OUTDOOR_LIVING','ELECTRIC_FAN','TABLECLOTH','COFFEE_MAKER','HOME_BED_AND_BATH','HOME_LIGHTING_AND_LAMPS','SMALL_HOME_APPLIANCES'],'HOME')
	df['Category'] = df['Category'].replace(['SHOES','PANTS','SHIRT','SHORTS','OUTERWEAR','SWEATSHIRT','HAT', 'SOCKSHOSIERY','UNDERWEAR','TECHNICAL_SPORT_SHOE'],'APPAREL')
	df['Category'] = df['Category'].replace(['OUTDOOR_RECREATION_PRODUCT','SPORTING_GOODS'],'SPORTS_OUTDOOR')
	df['Category'] = df['Category'].replace(['TEA','COFFEE'],'GROCERY')
	df['Category'] = df['Category'].replace(['AUTO_PART','HARDWARE','AUTO_ACESSORY','PRECISION_MEASURING','BUILDING_MATERIAL','AUTO_ACCESSORY', 'SCREWDRIVER', 'CAR_ALARM', 'MECHANICAL_COMPONENTS', 'TOOLS'],'AUTO_TOOLS')
	df['Category'] = df['Category'].replace(['WRITING_INSTRUMENT','PAPER_PRODUCT','BACKPACK','CARRYING_CASE_OR_BAG','CE_CARRYING_CASE_OR_BAG','OFFICE_PRODUCTS'],'OFFICE')
	df['Category'] = df['Category'].replace(['ABIS_DVD','TOYS_AND_GAMES','ABIS_MUSIC','DOWNLOADABLE_VIDEO_GAME','ART_AND_CRAFT_SUPPLY'],'ENTERTAINMENT')
	df['Category'] = df['Category'].replace(['ABIS_BOOK', 'BOOKS_1973_AND_LATER'],'BOOKS')
	df['Category'] = df['Category'].replace(['AV_FURNITURE','CELLULAR_PHONE_CASE','PHONE_ACCESSORY','PET_SUPPLIES','ACCESSORY','BAG','ACCESSORY_OR_PART_OR_SUPPLY', 'LUGGAGE', 'LAB_SUPPLY', 'CADDY', 'ABIS_GIFT_CARD', 'BISS'],'OTHER')
	df['Category'] = df['Category'].replace(['','unknown'],'UNKNOWN')

	# Fix formatting for category names
	df['Category'] = df['Category'].str.replace('_', ' & ').str.title()

	# Reduce Sellers
	seller_amazon = ['Amazon.com', 'AmazonWireless', 'Amazon.com Services LLC', 'Amazon']
	df.loc[df.Seller.isin(seller_amazon), 'Seller'] = 'Amazon'
	df.loc[~df.Seller.isin(seller_amazon), 'Seller'] = 'Third Party'

	# Simplify Condition column
	used = ['used verygood', 'used good', 'used mint']
	df.loc[df.Condition.isin(used), 'Condition'] = 'Used'
	df.Condition.loc[df.Condition == 'unknown'] = 'Unknown'
	df.Condition = df.Condition.str.replace('new', 'New')

	# Final dataframe
	df.head()

	# Connect to sql using sqlalchemy
	#engine = create_engine('mysql+pymysql://' + os.environ.get("MYSQL_USER") + ":" + os.environ.get("MYSQL_PASSWORD") + '@localhost:3306/amazon')

	# Export df to sql using df.to_sql
	#df.to_sql('purchases_airflow_aws', con=engine, if_exists = 'replace', index=False)
	
	# Export final dataframe to csv
	df.to_csv(csv_output_path, index = False)

t2 = PythonOperator(
	task_id = 'etl_amazon_purchases.csv',
	python_callable = etl_csv,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Connect to Postgres on AWS RDS and import output csv from previous task
# Unlike MySQL, Postgres will import column names in lowercase unless wrapped in quotes. 

t3 = PostgresOperator(
	task_id = 'create_table_postgres',
	postgres_conn_id = 'amazon_order_history_aws',
	sql = '''
	CREATE SCHEMA IF NOT EXISTS amazon;
	DROP TABLE IF EXISTS amazon.purchases_aws; 
	CREATE TABLE amazon.purchases_aws (
	"OrderID" int not null primary key,
	"OrderDate" date,
	"Category" varchar(50),
	"Condition" varchar(50),
	"Seller" varchar(50),
	"ListPricePerUnit" numeric(10,2),
	"PurchasePricePerUnit" numeric(10,2),
	"Quantity" int,
	"ShipDate" date,
	"Carrier" varchar(50),
	"ItemSubtotal" numeric(10,2),
	"Tax" numeric(10,2),
	"ItemTotal" numeric(10,2),
	"OrderYear" int,
	"OrderMonth" int,
	"OrderDay" int,
	"OrderDayIndex" int,
	"OrderDayName" varchar(50)
	);''',
	dag = dag
)

hook_copy_table = airflow.hooks.postgres_hook.PostgresHook('amazon_order_history_aws')

def import_csv_postgres():
	'''
	#Imports CSV into Postgres RDS
	'''
	sql = "DELETE FROM amazon.purchases_aws; COPY amazon.purchases_aws FROM STDIN WITH CSV DELIMITER ',' HEADER;"
	hook_copy_table.copy_expert(sql, csv_output_path, open = open)

t4 = PythonOperator(
	task_id = 'import_csv_postgres',
	python_callable = import_csv_postgres,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Run Jupyter Notebook locally

notebook_in_path = '/Users/amit/Coding/Projects/AmazonOrderHistoryAirflowAWS/AmazonOrderHistoryAirflowAWS_input.ipynb'
notebook_out_path = '/Users/amit/Coding/Projects/AmazonOrderHistoryAirflowAWS/AmazonOrderHistoryAirflowAWS_output.ipynb'

def run_notebook():
	pm.execute_notebook(notebook_in_path,notebook_out_path)


t5 = PythonOperator(
	task_id = 'run_notebook',
	python_callable = run_notebook,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Dependencies

t1 >> t2 >> t3 >> t4 >> t5
