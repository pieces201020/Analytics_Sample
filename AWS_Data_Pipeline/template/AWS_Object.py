'''
This script is for pulling data from multiple sources and loading them into AWS S3 and then to Redshift
Meanwhile make copies in S3

@author: bowei.zhang


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



Improvement:
1. Figure out how to count file when S3 -> Redshift for QA purpose
2. Add more automated SQL SP like merging from staging table to prod table for avoviding dups purpose
3. Check out more data pipeline PPT and URL to dig into it
'''
import os
import sys
import pg
import json
import boto
from boto import s3
import boto3
import csv
import glob
import logging



class AWS(object):
	def __init__(self,aws_config_file):
 #       ''' Initialize the AWS class with config'''     
		#config = None
		global s3Client
		region = 'us-east-1'    
		s3Client = boto.s3.connect_to_region(region)

		if aws_config_file == None:
			print '\n' + "Need input config file" + '\n'
			sys.exit(-1)

		else:
			self.getConfig(aws_config_file)
			# Path for temp folder 
			self.dataStagingPath = "%s/" % (config['s3Staging']['path'].rstrip("/"))
			if not self.dataStagingPath.startswith("s3://"):
				print "s3Staging.path must be a path to S3"
				sys.exit(-1)

			self.accessKey = config['s3Staging']['aws_access_key_id']
			self.secretKey = config['s3Staging']['aws_secret_access_key']

			# S3 location for data staging
			self.bucket = config['s3Staging']['bucket']
			self.key = config['s3Staging']['key']
			self.region = config['s3Staging']['region']

			# target to which we'll import data
			self.destConfig = config['copyTarget']

			self.dest_host = self.destConfig['clusterEndpoint']
			self.dest_port = self.destConfig['clusterPort']
			self.dest_db = self.destConfig['db']
			self.dest_schema = self.destConfig['schemaName']
			self.dest_table = self.destConfig['tableName']
			self.dest_staging_table = self.destConfig['staging_tableName']
			self.dest_user = self.destConfig['connectUser']
			self.dest_pwd = self.destConfig["connectPwd"]




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




	def upsert_data(self,conn):
		print '\n' + "=============================================================================================" + '\n'
		print "Upserting to Target Table" + '\n'
		print "Upserting %s.%s from %s" % (self.dest_schema, self.dest_table, self.dataStagingPath)

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
							region 'us-east-1'
							COMPUPDATE OFF STATUPDATE OFF;

							-- Update records 
							UPDATE %s.%s 
							SET 
							  timestamp_utc = s.timestamp_utc ,
							  timestamp_adjusted = s.timestamp_adjusted,
							  event_name = s.event_name ,
							  event_time_registered = s.event_time_registered,
							  geo_country = s.geo_country,
							  geo_region = s.geo_region,
							  geo_city = s.geo_city,
							  geo_lat = s.geo_lat,
							  geo_lon = s.geo_lon,
							  device_type = s.device_type,
							  device_os = s.device_os,
							  device_version = s.device_version,
							  device_id_adid = s.device_id_adid,
							  device_id_android_id = s.device_id_android_id,
							  device_id_kochava = s.device_id_kochava,
							  device_id_idfv = s.device_id_idfv,
							  device_id_idfa = s.device_id_idfa,
							  device_id_custom = s.device_id_custom,
							  app_id = s.app_id,
							  app_guid = s.app_guid,
							  request_ua = s.request_ua,
							  request_ip = s.request_ip,
							  attribution_timestamp = s.attribution_timestamp,
							  attribution_timestamp_adjusted = s.attribution_timestamp_adjusted,
							  attribution_matched_by = s.attribution_matched_by,
							  attribution_matched_to = s.attribution_matched_to,
							  attribution_network_id = s.attribution_network_id,
							  attribution_network = s.attribution_network,
							  attribution_tracker_id = s.attribution_tracker_id,
							  attribution_site_id = s.attribution_site_id,
							  attribution_tier = s.attribution_tier,
							  attribution_site = s.attribution_site,
							  attribution_tracker = s.attribution_tracker,
							  attribution_campaign_id = s.attribution_campaign_id,
							  attribution_campaign = s.attribution_campaign,
							  attribution_date_utc = s.attribution_date_utc,
							  attribution_date_adjusted = s.attribution_date_adjusted,
							  attribution_creative = s.attribution_creative,
							  attribution_seconds_since = s.attribution_seconds_since,
							  custom_dimensions = s.custom_dimensions,
							  dimension_data = s.dimension_data,
							  dimension_count = s.dimension_count,
							  dimension_sum = s.dimension_sum,
							  count = s.count
							FROM %s.%s s 
							WHERE %s.%s.timestamp_utc = s.timestamp_utc and %s.%s.device_id_kochava = s.device_id_kochava; 

							-- Insert records 
							INSERT INTO %s.%s 
							SELECT s.* FROM %s.%s s LEFT JOIN %s.%s
							ON s.device_id_kochava = %s.%s.device_id_kochava and s.timestamp_utc = %s.%s.timestamp_utc
							WHERE %s.%s.timestamp_utc IS NULL or %s.%s.device_id_kochava IS NULL  ;

							-- Drop the staging table
							DROP TABLE %s.%s; 

							-- End 
							end transaction ;"""

		try:
			conn.query(upsert_stmt % (#Create a staging table 
									  self.dest_schema, self.dest_staging_table,
									  self.dest_schema, self.dest_table,
									  #Load data into the staging table 
									  self.dest_schema, self.dest_staging_table,
									  self.dataStagingPath, 
									  self.accessKey, self.secretKey,
									  #Update records 
									  self.dest_schema, self.dest_table,
									  self.dest_schema, self.dest_staging_table,
									  self.dest_schema, self.dest_table,
									  self.dest_schema, self.dest_table,
									  #Insert records 
									  self.dest_schema, self.dest_table,
									  self.dest_schema, self.dest_staging_table,
									  self.dest_schema, self.dest_table,
									  self.dest_schema, self.dest_table,
									  self.dest_schema, self.dest_table,
									  self.dest_schema, self.dest_table,
									  self.dest_schema, self.dest_table,
									  #Drop the staging table
									  self.dest_schema, self.dest_staging_table

									  ))
			print '\n' + "Successfully upserted %s.%s from %s" % (self.dest_schema, self.dest_table, self.dataStagingPath)
		except Exception as e:
			print '\n'+'Had error upserting data to the Redshift'
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
		print "Start cleaning up S3 Data Temp Location %s" % (self.dataStagingPath)
		s3Info = self.tokeniseS3Path(self.dataStagingPath)
		
		stagingBucket = s3Client.get_bucket(s3Info[0])
		
		i = 0
		for key in stagingBucket.list(s3Info[1]):
			stagingBucket.delete_key(key)
			i = i + 1

		print '\n'+'Successfully delete ',i,' files S3 Data Temp Location'

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




	def upload2S3(self,filenames,local_path):
		print '\n' + "=============================================================================================" + '\n'
		print "Start uploading CSV into S3"


		s3 = boto3.client('s3')

		#for bucket in s3.buckets.all():
		#    print(bucket.name)


		if type(filenames) == str:         # single file (from webpage)
			local_filename = os.path.join(local_path, filenames)
			try:	
				s3.upload_file(local_filename, "%s" % self.bucket,  "%s/Temp/%s"  % (self.key,filenames))
				s3.upload_file(local_filename, "%s" % self.bucket, "%s/Archive/%s"  % (self.key,filenames))

				# Delete file in local
				os.remove(local_filename)      
				print '\n' + 'Successfully uploaded 1 file into S3 and also deleted in local'
			except Exception as e:	
				print '\n'+'Had error uploading data to S3'
				print logging.warning(e)
				sys.exit(-1)


		elif type(filenames) == list:                                 #  multiple files (from FTP)
			i = 0
			for filename in filenames:            
				local_filename = os.path.join(local_path, filename)
				try:	
					s3.upload_file(local_filename, "%s" % self.bucket,  "%s/Temp/%s"  % (self.key,filename))
					s3.upload_file(local_filename, "%s" % self.bucket, "%s/Archive/%s"  % (self.key,filename))
					i = i + 1
					# Delete file in local
					os.remove(local_filename)
				except Exception as e:	
					print '\n'+'Had error uploading data to S3'
					print logging.warning(e)
					sys.exit(-1)
				continue
		else:
			print '\n'+'There might not be file in local, please check'
			sys.exit(-1)


			print '\n' + 'Successfully upload ',i,' files into S3'





if __name__ == "__main__":
	pass


	




	