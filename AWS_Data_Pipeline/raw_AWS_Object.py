'''
This script is writen by Bowei Zhang for pulling data from multiple sources and loading them into AWS S3 and then to Redshift
Meanwhile make copies in S3


Usages:


	import AWS_Object

#The location where the AWS_config file is
	aws_config_file = 's3://config.json'

#The temporary location to store your file in local (files will be deleted after imprting)
	local_path  = '/home/ec2-user/test'

	AWS = AWS_Object.AWS(aws_config_file)


# upload file to S3 and delete in local
	if [os.path.basename(x) for x in glob.glob("%s/*.csv" % local_path)] == []:
		print '\n'+"There is no file in local, please check"
		sys.exit(-1)
	else:
		for filename in [os.path.basename(x) for x in glob.glob("%s/*.csv" % local_path)]:    # os.path.basename is to get rid of path before filename
			try:
				AWS.upload2S3(filename,local_path)
			except Exception as e:
				print '\n'+"Something wrong when uploading data into S3"
				print logging.warning(e)
				print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


	dest_conn = AWS.conn_to_rs()
	try:
		AWS.upsert_data(dest_conn)
	except Exception as e:
		print '\n'+"Something wrong when upserting data into Redshift"
		print logging.warning(e)
		print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"



	dest_conn.close()

	# Delete file on S3 staging folder
	AWS.s3Delete()

'''
import os
import sys
sys.path.append('/home/ec2-user/anaconda2/lib/python2.7/site-packages')
import pg
import json
import boto
from boto import s3
import boto3
import csv
import glob
import logging
from ftplib import FTP
#import psycopg2
import shutil
import zipfile
import time
from pandas import read_sql
from pandas import DataFrame



class AWS(object):
	def __init__(self,Current_path,local_path_list,aws_config_file):
 #       ''' Initialize the AWS class with config'''
		#config = None
		global s3Client
		region = 'us-east-1'
		s3Client = boto.s3.connect_to_region(region)



		self.dcm_local_path = local_path_list[0]
		self.delivery_workup_local_path = local_path_list[1]
		self.adobe_local_path = local_path_list[2]
		self.netmining_local_path = local_path_list[3]
		self.asst_demand_local_path = local_path_list[4]
		self.calendar_local_path = local_path_list[5]
		self.haircut_local_path = local_path_list[6]

		# DCM Delivey workup
		self.criteo_local_path = local_path_list[7]
		self.delivery_raw_local_path = local_path_list[8]
		self.facebook_local_path = local_path_list[9]
		self.gdn_local_path = local_path_list[10]


		self.current_path = Current_path

		self.category_brand_local_path = local_path_list[12]
		self.placement_target_local_path = local_path_list[13]
		self.delivery_adjustment_local_path = local_path_list[14]







		if aws_config_file == None:
			print '\n' + "Need input config file" + '\n'
			sys.exit(-1)

		else:
			self.getConfig(aws_config_file)
			# Path for temp folder
			self.dataStagingPath = config['s3Staging']['path_list'].split(',')





			self.rl_dcm_demand_raw_dataStagingPath =  "%s/" % (self.dataStagingPath[0].rstrip("/"))
			self.rl_dcm_delivery_workup_raw_dataStagingPath = "%s/" % (self.dataStagingPath[1].rstrip("/"))
			self.rl_adobe_raw_dataStagingPath = "%s/" % (self.dataStagingPath[2].rstrip("/"))
			self.rl_netmining_spend_raw_dataStagingPath = "%s/" % (self.dataStagingPath[3].rstrip("/"))
			self.rl_lookup_assist_view_demand_raw_dataStagingPath = "%s/" % (self.dataStagingPath[4].rstrip("/"))
			self.rl_lookup_calendar_raw_dataStagingPath = "%s/" % (self.dataStagingPath[5].rstrip("/"))
			self.rl_lookup_haircut_raw_dataStagingPath = "%s/" % (self.dataStagingPath[6].rstrip("/"))



			# DCM Delivey workup
			self.rl_criteo_raw_dataStagingPath = "%s/" % (self.dataStagingPath[7].rstrip("/"))
			self.rl_dcm_delivery_raw_dataStagingPath = "%s/" % (self.dataStagingPath[8].rstrip("/"))
			self.rl_dcm_facebook_raw_dataStagingPath = "%s/" % (self.dataStagingPath[9].rstrip("/"))
			self.rl_gdn_raw_dataStagingPath = "%s/" % (self.dataStagingPath[10].rstrip("/"))

			# Output
			self.rl_output_matched_order_dataStagingPath = "%s/" % (self.dataStagingPath[11].rstrip("/"))
			self.rl_output_merged_spend_revenue_dataStagingPath = "%s/" % (self.dataStagingPath[12].rstrip("/"))
			self.rl_output_omni_channel_dataStagingPath = "%s/" % (self.dataStagingPath[13].rstrip("/"))
			self.rl_output_final_dataStagingPath = "%s/" % (self.dataStagingPath[16].rstrip("/"))


			# More lookups
			self.rl_lookup_category_brand_dataStagingPath = "%s/" % (self.dataStagingPath[14].rstrip("/"))
			self.rl_lookup_placement_target_dataStagingPath = "%s/" % (self.dataStagingPath[15].rstrip("/"))
			self.rl_lookup_delivery_adjustment_dataStagingPath = "%s/" % (self.dataStagingPath[17].rstrip("/"))












			self.accessKey = config['s3Staging']['aws_access_key_id']
			self.secretKey = config['s3Staging']['aws_secret_access_key']

			# S3 location for data staging
			self.bucket = config['s3Staging']['bucket']
			self.key_list = config['s3Staging']['key_list'].split(',')
			self.path_list = config['s3Staging']['path_list'].split(',')
			self.region = config['s3Staging']['region']

			# target to which we'll import data
			self.destConfig = config['copyTarget']

			self.dest_host = self.destConfig['clusterEndpoint']
			self.dest_port = self.destConfig['clusterPort']
			self.dest_db = self.destConfig['db']
			self.dest_schema = self.destConfig['schemaName']
			self.dest_table_list = self.destConfig['tableName_list'].split(',')
			self.dest_staging_table_list = self.destConfig['staging_tableName_list'].split(',')

			self.dest_user = self.destConfig['connectUser']
			self.dest_pwd = self.destConfig["connectPwd"]

			self.rl_dcm_demand_raw_dest_table = self.dest_table_list[0]
			self.rl_dcm_demand_raw_dest_staging_table = self.dest_staging_table_list[0]

			self.rl_dcm_delivery_workup_raw_dest_table = self.dest_table_list[1]
			self.rl_dcm_delivery_workup_raw_dest_staging_table = self.dest_staging_table_list[1]

			self.rl_netmining_spend_raw_dest_table = self.dest_table_list[2]
			self.rl_netmining_spend_raw_dest_staging_table = self.dest_staging_table_list[2]

			self.rl_lookup_assist_view_demand_raw_dest_table = self.dest_table_list[3]
			self.rl_lookup_assist_view_demand_raw_dest_staging_table = self.dest_staging_table_list[3]

			self.rl_lookup_calendar_raw_dest_table = self.dest_table_list[4]
			self.rl_lookup_calendar_raw_dest_staging_table = self.dest_staging_table_list[4]

			self.rl_lookup_haircut_raw_dest_table = self.dest_table_list[5]
			self.rl_lookup_haircut_raw_dest_staging_table = self.dest_staging_table_list[5]

			self.rl_criteo_raw_dest_table = self.dest_table_list[6]
			self.rl_criteo_raw_dest_staging_table = self.dest_staging_table_list[6]

			self.rl_dcm_delivery_raw_dest_table = self.dest_table_list[7]
			self.rl_dcm_delivery_raw_dest_staging_table = self.dest_staging_table_list[7]

			self.rl_dcm_facebook_raw_dest_table = self.dest_table_list[8]
			self.rl_dcm_facebook_raw_dest_staging_table = self.dest_staging_table_list[8]

			self.rl_gdn_raw_dest_table = self.dest_table_list[9]
			self.rl_gdn_raw_dest_staging_table = self.dest_staging_table_list[9]

			self.rl_output_matched_order_dest_table = self.dest_table_list[10]
			self.rl_output_matched_order_dest_staging_table = self.dest_staging_table_list[10]

			self.rl_output_merged_spend_revenue_dest_table = self.dest_table_list[11]
			self.rl_output_merged_spend_revenue_dest_staging_table = self.dest_staging_table_list[11]

			self.rl_output_omni_channel_dest_table = self.dest_table_list[12]
			self.rl_output_omni_channel_dest_staging_table = self.dest_staging_table_list[12]



			self.rl_lookup_category_brand_dest_table = self.dest_table_list[13]
			self.rl_lookup_category_brand_dest_staging_table = self.dest_staging_table_list[13]

			self.rl_lookup_placement_target_dest_table = self.dest_table_list[14]
			self.rl_lookup_placement_target_dest_staging_table = self.dest_staging_table_list[14]

			self.rl_output_final_dest_table = self.dest_table_list[15]
			self.rl_output_final_dest_staging_table = self.dest_staging_table_list[15]


	def conn_to_rs(self):
		print '\n' + "=============================================================================================" + '\n'
		options = """keepalives=1 keepalives_idle=200 keepalives_interval=200
				 keepalives_count=6"""
		set_timeout_stmt = "set statement_timeout = 1200000"

		opt=options
		timeout=set_timeout_stmt



		rs_conn_string = """host=%s port=%s dbname=%s user=%s password=%s
							 %s""" % (self.dest_host, self.dest_port, self.dest_db, self.dest_user, self.dest_pwd, opt)
		print "Start connecting to %s:%s:%s as %s" % (self.dest_host, self.dest_port, self.dest_db, self.dest_user)
		try:
			rs_conn = pg.connect(dbname=rs_conn_string)
			rs_conn.query(timeout)
			print '\n' + "Successfully connected to %s:%s:%s as %s" % (self.dest_host, self.dest_port, self.dest_db, self.dest_user)
		except Exception as e:
			print '\n'+'Had error connecting to the Redshift'
			print logging.warning(e)
			sys.exit(-1)
		return rs_conn

	def query_redshift(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Start querying data from redshift" + '\n'
		try:
			#cur = conn.cursor()
			#cur.execute("select * from rl_lookup_calendar_raw;")
			#df_cal = DataFrame(cur.fetchall())

			self.df_raw_date = DataFrame(conn.query("select * from rl_lookup_calendar_raw;").namedresult())
			self.df_raw_dcm = DataFrame(conn.query("select * from rl_dcm_demand_raw;").namedresult())
			self.df_raw_per = DataFrame(conn.query("select * from rl_adobe_raw;").namedresult())
			self.df_raw_asst = DataFrame(conn.query("select * from rl_lookup_assist_view_demand_raw;").namedresult())




			#df_cal = read_sql("select * from rl_lookup_calendar_raw", con=conn)
			df_cal.to_csv('df_cal.csv',index=False,encoding='UTF-8')
			df_delivery_workup.to_csv('df_delivery_workup.csv',index=False,encoding='UTF-8')
			print '\n' + "Successfully querying data from redshift"
		except Exception as e:
			print '\n'+'Had error querying data from redshift'
			print logging.warning(e)


	def upsert_rl_dcm_demand_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_dcm_demand_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_dcm_demand_raw_dest_table, self.rl_dcm_demand_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 18
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.Order_Number = s.Order_Number and %s.%s.Activity_Date_Time = s.Activity_Date_Time;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_dcm_demand_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_demand_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_dcm_demand_raw_dest_staging_table,
									  self.rl_dcm_demand_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_dcm_demand_raw_dest_table,
									  self.dest_schema, self.rl_dcm_demand_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_demand_raw_dest_table,
									  self.dest_schema, self.rl_dcm_demand_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_dcm_demand_raw_dest_table,
									  self.dest_schema, self.rl_dcm_demand_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_dcm_demand_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_dcm_demand_raw_dest_table, self.rl_dcm_demand_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_dcm_demand_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)


	def upsert_rl_dcm_delivery_workup_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_dcm_delivery_workup_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_table, self.rl_dcm_delivery_workup_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.Date = s.Date and %s.%s.Creative_ID = s.Creative_ID;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_staging_table,
									  self.rl_dcm_delivery_workup_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_table,
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_table,
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_table,
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_dcm_delivery_workup_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_dcm_delivery_workup_raw_dest_table, self.rl_dcm_delivery_workup_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_dcm_delivery_workup_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)


	def upsert_rl_lookup_assist_view_demand_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_lookup_assist_view_demand_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_table, self.rl_lookup_assist_view_demand_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from 's3://360iralphlauren/etl/raw/other/asst_demand/assist_view_demand.csv'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.date = s.date and %s.%s.tactic = s.tactic;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_staging_table,
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_staging_table,
									  # self.rl_lookup_assist_view_demand_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_table,
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_staging_table,
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_table,
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_table,
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_lookup_assist_view_demand_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_lookup_assist_view_demand_raw_dest_table, self.rl_lookup_assist_view_demand_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_lookup_assist_view_demand_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)



	def upsert_rl_lookup_calendar_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_lookup_calendar_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_lookup_calendar_raw_dest_table, self.rl_lookup_calendar_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from 's3://360iralphlauren/etl/raw/other/calendar_lookup/Lookup.csv'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.date = s.date and %s.%s.week_name = s.week_name;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_staging_table,
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_staging_table,
									  # self.rl_lookup_calendar_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_table,
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_staging_table,
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_table,
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_table,
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_lookup_calendar_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_lookup_calendar_raw_dest_table, self.rl_lookup_calendar_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_lookup_calendar_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)





	def upsert_rl_lookup_haircut_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_lookup_haircut_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_lookup_haircut_raw_dest_table, self.rl_lookup_haircut_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from 's3://360iralphlauren/etl/raw/other/haircut_lookup/Haircuts.csv'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s._date = s._date and %s.%s.hair_cut = s.hair_cut;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_staging_table,
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_staging_table,
									  # self.rl_lookup_haircut_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_table,
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_staging_table,
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_table,
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_table,
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_lookup_haircut_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_lookup_haircut_raw_dest_table, self.rl_lookup_haircut_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_lookup_haircut_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)





	def upsert_rl_netmining_spend_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_netmining_spend_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_netmining_spend_raw_dest_table, self.rl_netmining_spend_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from 's3://360iralphlauren/etl/raw/netmining/netmining.csv'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s._date = s._date and %s.%s.tactic = s.tactic;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_netmining_spend_raw_dest_staging_table,
									  self.dest_schema, self.rl_netmining_spend_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_netmining_spend_raw_dest_staging_table,
									  # self.rl_netmining_spend_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_netmining_spend_raw_dest_table,
									  self.dest_schema, self.rl_netmining_spend_raw_dest_staging_table,
									  self.dest_schema, self.rl_netmining_spend_raw_dest_table,
									  self.dest_schema, self.rl_netmining_spend_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_netmining_spend_raw_dest_table,
									  self.dest_schema, self.rl_netmining_spend_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_netmining_spend_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_netmining_spend_raw_dest_table, self.rl_netmining_spend_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_netmining_spend_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)




	def upsert_rl_criteo_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_criteo_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_criteo_raw_dest_table, self.rl_criteo_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from 's3://360iralphlauren/etl/raw/dcm/delivery/criteo/criteo.csv'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 2
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.Day = s.Day and %s.%s.Hour = s.Hour;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_criteo_raw_dest_staging_table,
									  self.dest_schema, self.rl_criteo_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_criteo_raw_dest_staging_table,
									  # self.rl_criteo_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_criteo_raw_dest_table,
									  self.dest_schema, self.rl_criteo_raw_dest_staging_table,
									  self.dest_schema, self.rl_criteo_raw_dest_table,
									  self.dest_schema, self.rl_criteo_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_criteo_raw_dest_table,
									  self.dest_schema, self.rl_criteo_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_criteo_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_criteo_raw_dest_table, self.rl_criteo_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_criteo_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)





	def upsert_rl_dcm_delivery_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_dcm_delivery_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_dcm_delivery_raw_dest_table, self.rl_dcm_delivery_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 17
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s._Date = s._Date and %s.%s.Creative_ID = s.Creative_ID;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_staging_table,
									  self.rl_dcm_delivery_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_table,
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_table,
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_table,
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_dcm_delivery_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_dcm_delivery_raw_dest_table, self.rl_dcm_delivery_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_dcm_delivery_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)




	def upsert_rl_dcm_facebook_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_dcm_facebook_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_dcm_facebook_raw_dest_table, self.rl_dcm_facebook_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.Reporting_Starts = s.Reporting_Starts and %s.%s.Campaign = s.Campaign;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_staging_table,
									  self.rl_dcm_facebook_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_table,
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_staging_table,
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_table,
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_table,
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_dcm_facebook_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_dcm_facebook_raw_dest_table, self.rl_dcm_facebook_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_dcm_facebook_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)



	def upsert_rl_gdn_raw(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_gdn_raw Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_gdn_raw_dest_table, self.rl_gdn_raw_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 2
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.Day = s.Day and %s.%s.Final_URL = s.Final_URL;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_gdn_raw_dest_staging_table,
									  self.dest_schema, self.rl_gdn_raw_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_gdn_raw_dest_staging_table,
									  self.rl_gdn_raw_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_gdn_raw_dest_table,
									  self.dest_schema, self.rl_gdn_raw_dest_staging_table,
									  self.dest_schema, self.rl_gdn_raw_dest_table,
									  self.dest_schema, self.rl_gdn_raw_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_gdn_raw_dest_table,
									  self.dest_schema, self.rl_gdn_raw_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_gdn_raw_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_gdn_raw_dest_table, self.rl_gdn_raw_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_gdn_raw data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)


	def upsert_rl_output_matched_order(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_output_matched_order Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_output_matched_order_dest_table, self.rl_output_matched_order_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.date = s.date and %s.%s.creative_id = s.creative_id;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_output_matched_order_dest_staging_table,
									  self.dest_schema, self.rl_output_matched_order_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_output_matched_order_dest_staging_table,
									  self.rl_output_matched_order_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_output_matched_order_dest_table,
									  self.dest_schema, self.rl_output_matched_order_dest_staging_table,
									  self.dest_schema, self.rl_output_matched_order_dest_table,
									  self.dest_schema, self.rl_output_matched_order_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_output_matched_order_dest_table,
									  self.dest_schema, self.rl_output_matched_order_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_output_matched_order_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_output_matched_order_dest_table, self.rl_output_matched_order_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_output_matched_order data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)


	def upsert_rl_output_merged_spend_revenue(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_output_merged_spend_revenue Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_output_merged_spend_revenue_dest_table, self.rl_output_merged_spend_revenue_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.date = s.date and %s.%s.creative_id = s.creative_id;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_staging_table,
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_staging_table,
									  self.rl_output_merged_spend_revenue_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_table,
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_staging_table,
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_table,
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_table,
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_output_merged_spend_revenue_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_output_merged_spend_revenue_dest_table, self.rl_output_merged_spend_revenue_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_output_merged_spend_revenue data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)


	def upsert_rl_output_omni_channel(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_output_omni_channel Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_output_omni_channel_dest_table, self.rl_output_omni_channel_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.date = s.date and %s.%s.placement_id = s.placement_id;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_output_omni_channel_dest_staging_table,
									  self.dest_schema, self.rl_output_omni_channel_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_output_omni_channel_dest_staging_table,
									  self.rl_output_omni_channel_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_output_omni_channel_dest_table,
									  self.dest_schema, self.rl_output_omni_channel_dest_staging_table,
									  self.dest_schema, self.rl_output_omni_channel_dest_table,
									  self.dest_schema, self.rl_output_omni_channel_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_output_omni_channel_dest_table,
									  self.dest_schema, self.rl_output_omni_channel_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_output_omni_channel_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_output_omni_channel_dest_table, self.rl_output_omni_channel_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_output_omni_channel data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)





	def upsert_rl_lookup_category_brand(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_lookup_category_brand Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_lookup_category_brand_dest_table, self.rl_lookup_category_brand_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from 's3://360iralphlauren/etl/raw/other/category_brand_lookup/Unique_Category_Brand.csv'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.unique_id = s.unique_id and %s.%s.brand = s.brand;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_lookup_category_brand_dest_staging_table,
									  self.dest_schema, self.rl_lookup_category_brand_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_lookup_category_brand_dest_staging_table,
									  # self.rl_lookup_category_brand_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_lookup_category_brand_dest_table,
									  self.dest_schema, self.rl_lookup_category_brand_dest_staging_table,
									  self.dest_schema, self.rl_lookup_category_brand_dest_table,
									  self.dest_schema, self.rl_lookup_category_brand_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_lookup_category_brand_dest_table,
									  self.dest_schema, self.rl_lookup_category_brand_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_lookup_category_brand_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_lookup_category_brand_dest_table, self.rl_lookup_category_brand_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_lookup_category_brand data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)





	def upsert_rl_lookup_placement_target(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_lookup_placement_target Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_lookup_placement_target_dest_table, self.rl_lookup_placement_target_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from 's3://360iralphlauren/etl/raw/other/placement_target_lookup/Placement_Target.csv'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.placement_id = s.placement_id and %s.%s.target = s.target;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_lookup_placement_target_dest_staging_table,
									  self.dest_schema, self.rl_lookup_placement_target_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_lookup_placement_target_dest_staging_table,
									  # self.rl_lookup_placement_target_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_lookup_placement_target_dest_table,
									  self.dest_schema, self.rl_lookup_placement_target_dest_staging_table,
									  self.dest_schema, self.rl_lookup_placement_target_dest_table,
									  self.dest_schema, self.rl_lookup_placement_target_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_lookup_placement_target_dest_table,
									  self.dest_schema, self.rl_lookup_placement_target_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_lookup_placement_target_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_lookup_placement_target_dest_table, self.rl_lookup_placement_target_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_lookup_placement_target data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)









	def upsert_rl_output_final(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to rl_output_final Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.rl_output_final_dest_table, self.rl_output_final_dataStagingPath)

		upsert_stmt = """ -- Start
							begin transaction;

							-- Create a staging table
							CREATE TABLE %s.%s (LIKE %s.%s);

							-- Load data into the staging table
							copy %s.%s from '%s'
							credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
							timeformat 'auto'
							IGNOREHEADER 1
							delimiter ','
							CSV quote as '"'
							ACCEPTINVCHARS as '|'
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;



							-- Delete overlap records from current table


							DELETE FROM %s.%s USING %s.%s s
							WHERE  %s.%s.date = s.date and %s.%s.unique_id = s.unique_id;



							-- Insert records
							INSERT INTO %s.%s
							SELECT * FROM %s.%s;


							-- Drop the staging table
							DROP TABLE %s.%s;

							-- End
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table
									  self.dest_schema, self.rl_output_final_dest_staging_table,
									  self.dest_schema, self.rl_output_final_dest_table,
									  #Load data into the staging table
									  self.dest_schema, self.rl_output_final_dest_staging_table,
									  self.rl_output_final_dataStagingPath,
									  self.accessKey, self.secretKey,
									  #Delete overlap records from current table
									  self.dest_schema, self.rl_output_final_dest_table,
									  self.dest_schema, self.rl_output_final_dest_staging_table,
									  self.dest_schema, self.rl_output_final_dest_table,
									  self.dest_schema, self.rl_output_final_dest_table,
									  #Insert records
									  self.dest_schema, self.rl_output_final_dest_table,
									  self.dest_schema, self.rl_output_final_dest_staging_table,

									  #Drop the staging table
									  self.dest_schema, self.rl_output_final_dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema,self.rl_output_final_dest_table, self.rl_output_final_dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting rl_output_final data to the Redshift'
			print logging.warning(e)
			sys.exit(-1)









	def tokeniseS3Path(self,path):
		#pathElements = self.dataStagingPath.split('/')
		pathElements = path.split('/')
		bucketName = pathElements[2]
		prefix = "/".join(pathElements[3:])

		return (bucketName, prefix)

	def s3Delete(self):
		print '\n' + "=============================================================================================" + '\n'
	#	print "Start cleaning up S3 Data Temp Location %s and %s " % (self.rl_dcm_demand_raw_dataStagingPath,self.rl_dcm_delivery_raw_dataStagingPath)
		print "Start cleaning up S3 Data Temp Location %s " % (self.rl_adobe_raw_dataStagingPath)
	#	rl_dcm_demand_raw_s3Info = self.tokeniseS3Path(self.rl_dcm_demand_raw_dataStagingPath)
	#	rl_dcm_delivery_raw_s3Info = self.tokeniseS3Path(self.rl_dcm_delivery_raw_dataStagingPath)
		#rl_adobe_raw_s3Info = self.tokeniseS3Path(self.rl_adobe_raw_dataStagingPath)



		rl_dcm_demand_raw_s3Info = self.tokeniseS3Path(self.rl_dcm_demand_raw_dataStagingPath)
		rl_dcm_delivery_workup_s3Info = self.tokeniseS3Path(self.rl_dcm_delivery_workup_raw_dataStagingPath)
		rl_adobe_raw_s3Info = self.tokeniseS3Path(self.rl_adobe_raw_dataStagingPath)
		rl_netmining_spend_raw_s3Info = self.tokeniseS3Path(self.rl_netmining_spend_raw_dataStagingPath)
		rl_lookup_assist_view_demand_raw_s3Info = self.tokeniseS3Path(self.rl_lookup_assist_view_demand_raw_dataStagingPath)
		rl_lookup_calendar_raw_s3Info = self.tokeniseS3Path(self.rl_lookup_calendar_raw_dataStagingPath)
		rl_lookup_haircut_raw_s3Info = self.tokeniseS3Path(self.rl_lookup_haircut_raw_dataStagingPath)

		# DCM Delivey workup
		rl_criteo_s3Info = self.tokeniseS3Path(self.rl_criteo_raw_dataStagingPath)
		rl_dcm_delivery_raw_s3Info = self.tokeniseS3Path(self.rl_dcm_delivery_raw_dataStagingPath)
		rl_facebook_s3Info = self.tokeniseS3Path(self.rl_dcm_facebook_raw_dataStagingPath)
		rl_gdn_s3Info = self.tokeniseS3Path(self.rl_gdn_raw_dataStagingPath)



		# Output
		rl_output_matched_order_s3Info = self.tokeniseS3Path(self.rl_output_matched_order_dataStagingPath)
		rl_output_merged_spend_revenue_s3Info = self.tokeniseS3Path(self.rl_output_merged_spend_revenue_dataStagingPath)
		rl_output_omni_channel_s3Info = self.tokeniseS3Path(self.rl_output_omni_channel_dataStagingPath)
		rl_output_final_s3Info = self.tokeniseS3Path(self.rl_output_final_dataStagingPath)


		stagingBucket = s3Client.get_bucket(rl_adobe_raw_s3Info[0])

		# i = 0
		# for key in stagingBucket.list(rl_dcm_demand_raw_s3Info[1]):
		# 	stagingBucket.delete_key(key)
		# 	i = i + 1

		# print '\n'+'Successfully delete ',i,' files in S3 rl_dcm_demand_raw Temp Location'

		# j = 0
		# for key in stagingBucket.list(rl_dcm_delivery_raw_s3Info[1]):
		# 	stagingBucket.delete_key(key)
		# 	j = j + 1

		# print '\n'+'Successfully delete ',j,' files in  S3 rl_dcm_delivery_raw Temp Location'

		k = 0
		for key in stagingBucket.list(rl_adobe_raw_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_dcm_demand_raw_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_netmining_spend_raw_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_lookup_assist_view_demand_raw_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_criteo_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_dcm_delivery_raw_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_facebook_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_gdn_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_output_matched_order_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_output_merged_spend_revenue_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_output_omni_channel_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

		k = 0
		for key in stagingBucket.list(rl_output_final_s3Info[1]):
			stagingBucket.delete_key(key)
			k = k + 1

#
		print '\n'+'Successfully delete  files in S3 '

	def getConfig(self,path):


		global config

		if path.startswith("s3://"):
			# download the configuration from s3
			s3Info = self.tokeniseS3Path(path)

			bucket = s3Client.get_bucket(s3Info[0])
			key = bucket.get_key(s3Info[1])

			configContents = key.get_contents_as_string()
			config = json.loads(configContents)
		else:
			with open(path) as f:
				config = json.load(f)




	def upload2S3(self,ftp_add_list,local_path_list):
		print '\n' + "=============================================================================================" + '\n'
		print "Start uploading data into S3"


		s3 = boto3.client('s3')

		current_time = time.strftime("%Y%m%d", time.localtime())
		#for bucket in s3.buckets.all():
		#    print(bucket.name)
		j = 0       # folder counter
		for filenames in ftp_add_list:
			if type(filenames) == str:         # single file
				local_filename = os.path.join(local_path_list[j], filenames)
				try:
					s3.upload_file(local_filename, "%s" % self.bucket,  "%s/Temp/%s"  % (self.key_list[j],filenames))
					s3.upload_file(local_filename, "%s" % self.bucket, "%s/Archive/%s_%s"  % (self.key_list[j],filenames))

					# Delete file in local
					os.remove(local_filename)
					print '\n' + 'Successfully uploaded 1 file into %s and also deleted in %s' % (self.path_list[j], local_path_list[j])
				except Exception as e:
					print '\n'+'Had error uploading %s to S3' % (local_path_list[j])
					print logging.warning(e)
					sys.exit(-1)
				j = j + 1


			elif type(filenames) == list:                                 #  multiple files
				i = 0                # file counter
				for filename in filenames:
					local_filename = os.path.join(local_path_list[j], filename)
					try:
						s3.upload_file(local_filename, "%s" % self.bucket,  "%s/Temp/%s"  % (self.key_list[j],filename))
						s3.upload_file(local_filename, "%s" % self.bucket, "%s/Archive/%s"  % (self.key_list[j],filename))
						i = i + 1
						# Delete file in local
						os.remove(local_filename)
					except Exception as e:
						print '\n'+'Had error uploading %s to S3' % (local_path_list[j])
						print logging.warning(e)
						sys.exit(-1)
					continue
				print '\n' + 'Successfully uploaded', i ,'files into %s and also deleted in %s' % (self.path_list[j], local_path_list[j])
				j = j + 1
			else:
				print '\n'+'There might not be file in %s, please check' % (local_path_list[j])
				sys.exit(-1)







	def download_from_FTP(self,ftp_site,ftp_user,ftp_pw,ftp_path_list,local_path_list):
		print '\n' + "=============================================================================================" + '\n'
		print "Start downloading CSV from FTP"
		try:
			ftp = FTP(ftp_site)
			ftp.login(user=ftp_user, passwd = ftp_pw)
			print '\n'+'Successfully connect to the FTP'

		except Exception as e:
			print '\n'+'Had error connecting to the FTP'
			logging.warning(e)



		ftp_add_list = []

		#ftp.retrlines('LIST')     # list directory contents
		j = 0      # folder counter

		for ftp_path in ftp_path_list:
			ftp.cwd(ftp_path)
			filenames = ftp.nlst() # get filenames within the directory
			#print filenames

			ftp_add_list.append(filenames)

			i = 0      # file counter
			# Download files from FTP folder to local Temp and Archive
			for filename in filenames:
				i = i + 1
				local_filename = os.path.join(local_path_list[j], filename)
				file_Temp = open(local_filename, 'wb')
				ftp.retrbinary('RETR '+ filename, file_Temp.write)
				file_Temp.close()

			print '\n' + 'Successfully download ',i,' files into %s' % (local_path_list[j])

			#Delete files in FTP folder
			for filename in filenames:
				try:
					ftp.delete(filename)
				except Exception:
					ftp.rmd(filename)
			print '\n'+'Successfully delete ',i,' files in %s' % (ftp_path)

			j = j + 1








			print '\n'+'Closing FTP connection'

		return ftp_add_list              # A list of list




	def adobe_download_from_FTP(self,ftp_site,ftp_user,ftp_pw,adobe_local_path,adobe_file):
		print '\n' + "=============================================================================================" + '\n'
		print "Start downloading CSV from FTP"
		try:
			ftp = FTP(ftp_site)
			ftp.login(user=ftp_user, passwd = ftp_pw)
			print '\n'+'Successfully connect to the FTP'

		except Exception as e:
			print '\n'+'Had error connecting to the FTP'
			logging.warning(e)



		local_filename = os.path.join(adobe_local_path, adobe_file)
		file_Temp = open(local_filename, 'wb')
		ftp.retrbinary('RETR '+ adobe_file, file_Temp.write)
		file_Temp.close()
		print '\n' + 'Successfully download Adobe files into %s' % (adobe_local_path)


		try:
			ftp.delete(adobe_file)
		except Exception:
			ftp.rmd(adobe_file)

		print '\n'+'Successfully delete Adobe files in FTP'



	def adobe_upload2S3(self,adobe_local_path,unzip):
		print '\n' + "=============================================================================================" + '\n'
		print "Start uploading Adobe data into S3"


		s3 = boto3.client('s3')

		current_time = time.strftime("%Y%m%d", time.localtime())

		local_filename = os.path.join(adobe_local_path, unzip)
		try:
			s3.upload_file(local_filename, "%s" % self.bucket,  "etl/raw/adobe/Temp/%s"  % (unzip))
			s3.upload_file(local_filename, "%s" % self.bucket, "etl/raw/adobe/Archive/%s_%s"  % (current_time,unzip))

			# Delete file in local
			os.remove(local_filename)
			print '\n' + 'Successfully uploaded Adobe file into etl/raw/adobe/Archive/ and also deleted in %s' % (adobe_local_path)
		except Exception as e:
			print '\n'+'Had error uploading %s to S3' % (adobe_local_path)
			print logging.warning(e)
			sys.exit(-1)


	def unzip(self,inpath,infile):
		full_infile = os.path.join(inpath, infile)

		with zipfile.ZipFile(full_infile) as zip_file:
			for member in zip_file.namelist():
				filename = os.path.basename(member)
				# skip directories
				if not filename:
					continue

				# copy file (taken from zipfile's extract)
				source = zip_file.open(member)
				target = file(os.path.join(inpath, filename), "wb")
				with source, target:
					shutil.copyfileobj(source, target)

		os.remove(full_infile)

		return filename






	def s3_download(self):
		print '\n' + "=============================================================================================" + '\n'
		print "Start get data from S3 to local"
		rl_dcm_demand_raw_s3Info = self.tokeniseS3Path(self.rl_dcm_demand_raw_dataStagingPath)
		rl_dcm_delivery_workup_s3Info = self.tokeniseS3Path(self.rl_dcm_delivery_workup_raw_dataStagingPath)
		rl_adobe_raw_s3Info = self.tokeniseS3Path(self.rl_adobe_raw_dataStagingPath)
		rl_netmining_spend_raw_s3Info = self.tokeniseS3Path(self.rl_netmining_spend_raw_dataStagingPath)
		rl_lookup_assist_view_demand_raw_s3Info = self.tokeniseS3Path(self.rl_lookup_assist_view_demand_raw_dataStagingPath)
		rl_lookup_calendar_raw_s3Info = self.tokeniseS3Path(self.rl_lookup_calendar_raw_dataStagingPath)
		rl_lookup_haircut_raw_s3Info = self.tokeniseS3Path(self.rl_lookup_haircut_raw_dataStagingPath)

		# DCM Delivey workup
		rl_criteo_s3Info = self.tokeniseS3Path(self.rl_criteo_raw_dataStagingPath)
		rl_dcm_delivery_raw_s3Info = self.tokeniseS3Path(self.rl_dcm_delivery_raw_dataStagingPath)
		rl_facebook_s3Info = self.tokeniseS3Path(self.rl_dcm_facebook_raw_dataStagingPath)
		rl_gdn_s3Info = self.tokeniseS3Path(self.rl_gdn_raw_dataStagingPath)





		rl_lookup_category_brand_s3Info = self.tokeniseS3Path(self.rl_lookup_category_brand_dataStagingPath)
		rl_lookup_placement_target_s3Info = self.tokeniseS3Path(self.rl_lookup_placement_target_dataStagingPath)
		rl_lookup_delivery_adjustment_s3Info = self.tokeniseS3Path(self.rl_lookup_delivery_adjustment_dataStagingPath)






		stagingBucket = s3Client.get_bucket(rl_adobe_raw_s3Info[0])


		s3 = boto3.client('s3')



		for key in stagingBucket.list(rl_dcm_demand_raw_s3Info[1]):
			dcm = key
			#print dcm
			#print type(dcm)

			#print str(dcm.key)
			#print type(str(dcm.key))

		for key in stagingBucket.list(rl_lookup_assist_view_demand_raw_s3Info[1]):
			asst_demand = key

		#for key in stagingBucket.list(rl_dcm_delivery_workup_s3Info[1]):
		#	delivery_workup = key

		for key in stagingBucket.list(rl_adobe_raw_s3Info[1]):
			adobe = key

		for key in stagingBucket.list(rl_netmining_spend_raw_s3Info[1]):
			netmining = key

		for key in stagingBucket.list(rl_lookup_calendar_raw_s3Info[1]):
			calendar = key

		for key in stagingBucket.list(rl_lookup_haircut_raw_s3Info[1]):
			haircut= key

		# DCM Delivey workup
		for key in stagingBucket.list(rl_criteo_s3Info[1]):
			criteo= key

		for key in stagingBucket.list(rl_dcm_delivery_raw_s3Info[1]):
			delivery_raw= key

		for key in stagingBucket.list(rl_facebook_s3Info[1]):
			facebook= key

		for key in stagingBucket.list(rl_gdn_s3Info[1]):
			gdn= key



		for key in stagingBucket.list(rl_lookup_category_brand_s3Info[1]):
			category_brand= key

		for key in stagingBucket.list(rl_lookup_placement_target_s3Info[1]):
			placement_target= key

		for key in stagingBucket.list(rl_lookup_delivery_adjustment_s3Info[1]):
			delivery_adjustment= key


		try:
			#s3.upload_file(local_filename, "%s" % self.bucket,  "etl/raw/adobe/Temp/%s"  % (unzip))
			#s3.upload_file(local_filename, "%s" % self.bucket, "etl/raw/adobe/Archive/%s_%s"  % (current_time,unzip))

			#s3.download_file(stagingBucket, str(dcm.key),  "%s/%s"  % (self.dcm_local_path,str(dcm.key)))

			os.chdir(self.dcm_local_path)
			dcm.get_contents_to_filename(str(dcm.key).replace('/', '_'))

			#os.chdir(self.delivery_workup_local_path)
			#delivery.get_contents_to_filename(str(delivery_workup.key).replace('/', '_'))

			os.chdir(self.adobe_local_path)
			adobe.get_contents_to_filename(str(adobe.key).replace('/', '_'))

			os.chdir(self.netmining_local_path)
			netmining.get_contents_to_filename(str(netmining.key).replace('/', '_'))

			os.chdir(self.asst_demand_local_path)
			asst_demand.get_contents_to_filename(str(asst_demand.key).replace('/', '_'))

			os.chdir(self.calendar_local_path)
			calendar.get_contents_to_filename(str(calendar.key).replace('/', '_'))

			os.chdir(self.haircut_local_path)
			haircut.get_contents_to_filename(str(haircut.key).replace('/', '_'))


			# DCM Delivey workup
			os.chdir(self.criteo_local_path)
			criteo.get_contents_to_filename(str(criteo.key).replace('/', '_'))

			os.chdir(self.delivery_raw_local_path)
			delivery_raw.get_contents_to_filename(str(delivery_raw.key).replace('/', '_'))

			os.chdir(self.facebook_local_path)
			facebook.get_contents_to_filename(str(facebook.key).replace('/', '_'))

			os.chdir(self.gdn_local_path)
			gdn.get_contents_to_filename(str(gdn.key).replace('/', '_'))



			os.chdir(self.category_brand_local_path)
			category_brand.get_contents_to_filename(str(category_brand.key).replace('/', '_'))


			os.chdir(self.placement_target_local_path)
			placement_target.get_contents_to_filename(str(placement_target.key).replace('/', '_'))

			os.chdir(self.delivery_adjustment_local_path)
			delivery_adjustment.get_contents_to_filename(str(delivery_adjustment.key).replace('/', '_'))
			# Delete file in local

			print '\n' + 'Successfully download  file from s3 to local'
		except Exception as e:
			print '\n'+'Had error download file from s3 to local'
			print logging.warning(e)
			sys.exit(-1)



	def output_uploadS3(self):
		print '\n' + "=============================================================================================" + '\n'
		print "Start uploading output into S3"


		s3 = boto3.client('s3')


		for f in os.listdir(self.current_path):
			if 'MatchedOrders' in f:
				MatchedOrders = f

			elif 'MergedSpendRevenue' in f:
				MergedSpendRevenue = f

			elif 'Omni' in f:
				Omni = f

			elif 'workup' in f:
				workup = f

			elif 'final' in f:
				final = f
			else:
				print "There is no output in local"

		matchedOrder_local_filename = os.path.join(self.current_path, MatchedOrders)
		MergedSpendRevenue_local_filename = os.path.join(self.current_path, MergedSpendRevenue)
		Omni_local_filename = os.path.join(self.current_path, Omni)
		workup_local_filename = os.path.join(self.current_path, workup)
		final_local_filename = os.path.join(self.current_path, final)


		try:
			s3.upload_file(matchedOrder_local_filename, "%s" % self.bucket,  "etl/output/matched_order/%s"  % (MatchedOrders))
			s3.upload_file(MergedSpendRevenue_local_filename, "%s" % self.bucket,  "etl/output/merged_spend_revenue/%s"  % (MergedSpendRevenue))
			s3.upload_file(Omni_local_filename, "%s" % self.bucket,  "etl/output/omni_channel/%s"  % (Omni))
			s3.upload_file(workup_local_filename, "%s" % self.bucket,  "etl/output/delivery_workup/%s"  % (workup))
			s3.upload_file(final_local_filename, "%s" % self.bucket,  "etl/output/final/%s"  % (final))



			# Delete file in local
			os.remove(matchedOrder_local_filename)
			os.remove(MergedSpendRevenue_local_filename)
			os.remove(Omni_local_filename)
			os.remove(workup_local_filename)
			os.remove(final_local_filename)

			print '\n' + 'Successfully uploaded output into etl/output/ and also deleted in local'
		except Exception as e:
			print '\n'+'Had error uploading output to S3'
			print logging.warning(e)
			sys.exit(-1)






	def csv_uploadS3(self):
		print '\n' + "=============================================================================================" + '\n'
		print "Start uploading csv into S3"


		s3 = boto3.client('s3')




		criteo_local_filename = os.path.join(self.criteo_local_path, r'criteo.csv')
		asst_view_local_filename = os.path.join(self.asst_demand_local_path, r'assist_view_demand.csv')
		netmining_local_filename = os.path.join(self.netmining_local_path,r'netmining.csv')




		try:
			s3.upload_file(criteo_local_filename, "%s" % self.bucket,  "etl/raw/dcm/delivery/criteo/criteo.csv" )
			s3.upload_file(asst_view_local_filename, "%s" % self.bucket,  "etl/raw/other/asst_demand/assist_view_demand.csv" )
			s3.upload_file(netmining_local_filename, "%s" % self.bucket,  "etl/raw/netmining/netmining.csv" )





			os.remove(criteo_local_filename)
			os.remove(asst_view_local_filename)
			os.remove(netmining_local_filename)

			print '\n' + 'Successfully uploaded csv into S3 and also deleted in local'
		except Exception as e:
			print '\n'+'Had error uploading csv to S3'
			print logging.warning(e)
			sys.exit(-1)











if __name__ == "__main__":
	pass
