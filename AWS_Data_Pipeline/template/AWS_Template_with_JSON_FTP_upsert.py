'''
This script is writen by Bowei Zhang for pulling data from multiple sources and loading them into AWS S3 and then to Redshift
Meanwhile make copies in S3
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
import requests
from ftplib import FTP


s3Client = None
config = None
region = None
bucket = None
key = None
aws_config_file = None



options = """keepalives=1 keepalives_idle=200 keepalives_interval=200 
			 keepalives_count=6"""
set_timeout_stmt = "set statement_timeout = 1200000"


copy_stmt = """copy %s.%s
			   from '%s' credentials 
			   'aws_access_key_id=%s;aws_secret_access_key=%s'
			   dateformat 'auto' 
			   IGNOREHEADER 1  
			   delimiter ',' ;"""

delete_stmt = """delete from %s.%s;"""

upsert_stmt = """ -- Start 
					begin transaction; 

					-- Create a staging table 
					CREATE TABLE %s.%s (LIKE %s.%s);

					-- Load data into the staging table 
					copy %s.%s from '%s' 
					credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' 
					dateformat 'auto' 
					IGNOREHEADER 1  
					delimiter ',' 
					region 'us-east-1'
					COMPUPDATE OFF STATUPDATE OFF;

					-- Update records 
					UPDATE %s.%s 
					SET 
					users = s.users, 
					events = s.events, 
					connections = s.connections, 
					invites = s.invites
					FROM %s.%s s 
					WHERE %s.%s._date = s._date; 

					-- Insert records 
					INSERT INTO %s.%s 
					SELECT s.* FROM %s.%s s LEFT JOIN %s.%s
					ON s._date = %s.%s._date
					WHERE %s.%s._date IS NULL;

					-- Drop the staging table
					DROP TABLE %s.%s; 

					-- End 
					end transaction ;"""


def conn_to_rs(host, port, db, usr, pwd, opt=options, timeout=set_timeout_stmt):
	print '\n' + "=============================================================================================" + '\n'
	rs_conn_string = """host=%s port=%s dbname=%s user=%s password=%s 
						 %s""" % (host, port, db, usr, pwd, opt)
	print "Start connecting to %s:%s:%s as %s" % (host, port, db, usr)
	try:
		rs_conn = pg.connect(dbname=rs_conn_string) 
		rs_conn.query(timeout)
		print '\n' + "Successfully connected to %s:%s:%s as %s" % (host, port, db, usr)
	except Exception as e:
		print '\n'+'Had error connecting to the Redshift'
		logging.warning(e)
	return rs_conn

'''
# These SQLs are replaced by Upsert function and not use anymore


def copy_data(conn, aws_access_key_id, aws_secret_key,  dataStagingPath, schema_name, table_name):
	print '\n' + "=============================================================================================" + '\n'
	print "Importing to Target Table" + '\n'
	print "Importing %s.%s from %s" % (schema_name, table_name, dataStagingPath)
	try:
		conn.query(copy_stmt % (schema_name, table_name, dataStagingPath, aws_access_key_id, aws_secret_key))
		print '\n' + "Successfully imported %s.%s from %s" % (schema_name, table_name, dataStagingPath)
	except Exception as e:
		print '\n'+'Had error copying data to the Redshift'
		logging.warning(e)

def delete_data(conn, schema_name, table_name):
	print '\n' + "=============================================================================================" + '\n'
	print "Deleting %s.%s" % (schema_name, table_name)
	conn.query(delete_stmt % (schema_name, table_name))
'''


def upsert_data(conn, aws_access_key_id, aws_secret_key, dataStagingPath, schema_name, table_name,staging_table_name):
	print '\n' + "=============================================================================================" + '\n'
	print "Upserting to Target Table" + '\n'
	print "Upserting %s.%s from %s" % (schema_name, table_name, dataStagingPath)
	try:
		conn.query(upsert_stmt % (#Create a staging table 
								  schema_name, staging_table_name, 
								  schema_name, table_name,
								  #Load data into the staging table 
								  schema_name, staging_table_name,
								  dataStagingPath, 
								  aws_access_key_id, aws_secret_key,
								  #Update records 
								  schema_name, table_name,
								  schema_name, staging_table_name,
								  schema_name, table_name,
								  #Insert records 
								  schema_name, table_name,
								  schema_name, staging_table_name,
								  schema_name, table_name,
								  schema_name, table_name,
								  schema_name, table_name,
								  #Drop the staging table
								  schema_name, staging_table_name

								  ))
		print '\n' + "Successfully upserted %s.%s from %s" % (schema_name, table_name, dataStagingPath)
	except Exception as e:
		print '\n'+'Had error upserting data to the Redshift'
		logging.warning(e)

def tokeniseS3Path(path):
	pathElements = path.split('/')
	bucketName = pathElements[2]
	prefix = "/".join(pathElements[3:])
	
	return (bucketName, prefix)

def s3Delete(stagingPath):
	print '\n' + "=============================================================================================" + '\n'
	print "Start cleaning up S3 Data Temp Location %s" % (stagingPath)
	s3Info = tokeniseS3Path(stagingPath)
	
	stagingBucket = s3Client.get_bucket(s3Info[0])
	
	i = 0
	for key in stagingBucket.list(s3Info[1]):
		stagingBucket.delete_key(key)
		i = i + 1

	print '\n'+'Successfully delete ',i,' files S3 Data Temp Location'

def getConfig(path):
	# datetime alias for operations

	global config
	
	if path.startswith("s3://"):
		# download the configuration from s3
		s3Info = tokeniseS3Path(path)
		
		bucket = s3Client.get_bucket(s3Info[0])
		key = bucket.get_key(s3Info[1])
	
		configContents = key.get_contents_as_string()
		config = json.loads(configContents)
	else:
		with open(path) as f:
			config = json.load(f)


def upload2S3(filenames,local_path,bucket,key):
	print '\n' + "=============================================================================================" + '\n'
	print "Start uploading CSV into S3"


	s3 = boto3.client('s3')

	#for bucket in s3.buckets.all():
	#    print(bucket.name)


	if type(filenames) == str:         # single file from webpage
		local_filename = os.path.join(local_path, filenames)
		try:	
			s3.upload_file(local_filename, "%s" % bucket,  "%s/Temp/%s"  % (key,filenames))
			s3.upload_file(local_filename, "%s" % bucket, "%s/Archive/%s"  % (key,filenames))

			# Delete file in local
			os.remove(local_filename)
			print '\n' + 'Successfully upload 1 file into S3'
		except Exception as e:	
			print '\n'+'Had error uploading data to S3'
			logging.warning(e)


	else:                                 #  multiple files from FTP
		i = 0
		for filename in filenames:            
			local_filename = os.path.join(local_path, filename)
			try:	
				s3.upload_file(local_filename, "%s" % bucket,  "%s/Temp/%s"  % (key,filename))
				s3.upload_file(local_filename, "%s" % bucket, "%s/Archive/%s"  % (key,filename))
				i = i + 1
				# Delete file in local
				os.remove(local_filename)
			except Exception as e:	
				print '\n'+'Had error uploading data to S3'
				logging.warning(e)
				continue




		print '\n' + 'Successfully upload ',i,' files into S3'


def download_JSON(url,local_path):
	print '\n' + "=============================================================================================" + '\n'
	print "Start downloading JSON from webpage and convert it into CSV"


	try:
		response = requests.get(url, verify=False)
	except requests.exceptions.RequestException as e:
		print e
		sys.exit(1)

	data = response.json()   # data type is dict


	#date = data['this_week']['date'].replace('/','_')   # in order to write into file name since '/' is not allowed in file name

	#Usually download from webpage only have one single file, but in case have multiple files, I just named "filenames"
	#in order to keep consistency with varible name since there are usually multiple files from FTP 
	filenames = "detailed.csv"                                    
	local_filenames = os.path.join(local_path, filenames)
	z = open(local_filenames, "wb+")
	f = csv.writer(z)

	# Write CSV Header
	CSV_header = [	
					"date", 
					"users",
					"events",
					"connections",
					"invites"
					]

	f.writerow(CSV_header)


	i = 0          #conuter for week
	for i in xrange(len(data['users']['by_week'])):
		f.writerow([
					data['users']['by_week'][i]['week'],
					data['users']['by_week'][i]['count'],
					data['events']['by_week'][i]['count'],
					data['connections']['by_week'][i]['count'],
					data['invites']['by_week'][i]['count']
					])
		i = i + 1
	z.close()


	print '\n' + 'Successfully download files from webpage'

	return filenames


def download_from_FTP(ftp_site,ftp_user,ftp_pw,ftp_path,local_path):
	print '\n' + "=============================================================================================" + '\n'
	print "Start downloading CSV from FTP"
	try:
		ftp = FTP(ftp_site)
		ftp.login(user=ftp_user, passwd = ftp_pw)
		print '\n'+'Successfully connect to the FTP'

	except Exception as e:
		print '\n'+'Had error connecting to the FTP'
		logging.warning(e)





	ftp.retrlines('LIST')     # list directory contents 
	ftp.cwd(ftp_path)

	filenames = ftp.nlst() # get filenames within the directory
	print filenames


	i = 0

	# Download files from FTP folder to local Temp and Archive
	for filename in filenames:
		i = i + 1
		local_filename = os.path.join(local_path, filename)
		file_Temp = open(local_filename, 'wb')
		ftp.retrbinary('RETR '+ filename, file_Temp.write)
		file_Temp.close()




	print '\n' + 'Successfully download ',i,' files into local folder'

	# Delete files in FTP folder
	for filename in filenames:
		try:
			ftp.delete(filename)
		except Exception:
			ftp.rmd(filename)

	print '\n'+'Successfully delete ',i,' files in FTP folder'
	print '\n'+'Closing FTP connection'

	return filenames



def main(*args):


	global region
	region = 'us-east-1'
	
	global s3Client    
	s3Client = boto.s3.connect_to_region(region)

	global bucket
	global key
	#global aws_config_file
	#aws_config_file = aws_config_file

	# load the configuration
	getConfig(aws_config_file)


	# Path for temp folder 
	dataStagingPath = "%s/" % (config['s3Staging']['path'].rstrip("/"))
	if not dataStagingPath.startswith("s3://"):
		print "s3Staging.path must be a path to S3"
		sys.exit(-1)

	bucket = config['s3Staging']['bucket']
	key = config['s3Staging']['key']
	
###############################################################################################
###################    Different Data Sources (Could only choose ONE)       ###################
###############################################################################################
	if data_source == 'URL_JSON':
		# Download JSON file from web	
		filenames = download_JSON(url,local_path)
	elif data_source == 'FTP':	
		# Download CSV files from ftp 
		filenames = download_from_FTP(ftp_site,ftp_user,ftp_pw,ftp_path,local_path)


###############################################################################################
#################################    AWS Part       ###########################################
###############################################################################################

	# upload file to S3 and delete in local
	upload2S3(filenames,local_path,bucket,key)

	
	accessKey = config['s3Staging']['aws_access_key_id']
	secretKey = config['s3Staging']['aws_secret_access_key']


	# target to which we'll import data
	destConfig = config['copyTarget']

	dest_host = destConfig['clusterEndpoint']
	dest_port = destConfig['clusterPort']
	dest_db = destConfig['db']
	dest_schema = destConfig['schemaName']
	dest_table = destConfig['tableName']
	dest_staging_table = destConfig['staging_tableName']
	dest_user = destConfig['connectUser']
	dest_pwd = destConfig["connectPwd"]


	


	dest_conn = conn_to_rs(dest_host, dest_port, dest_db, dest_user,
							  dest_pwd)		
	try:
		upsert_data(dest_conn, accessKey, secretKey,
					   dataStagingPath,
					  dest_schema, dest_table,dest_staging_table)
	except Exception as e:
		print '\n'+"Something wrong when interting data into Redshift"
		print logging.warning(e)
		print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"



	dest_conn.close()
	

	s3Delete(dataStagingPath)
	


if __name__ == "__main__":


	# Log 
	logging.basicConfig(filename='Errors.txt',stream=sys.stdout,level=logging.DEBUG)
	orig_stdout = sys.stdout
	logs = file('Logs'+'.txt', 'w')    
	sys.stdout = logs
###############################################################################################
#################################    Make Change Below        #################################
###############################################################################################


#The location where the AWS_config file is
	aws_config_file = 's3://360ipernod/test/Config/AWS_Config_Template_test.json'

#The temporary location to store your file in local (files will be deleted after imprting)
	local_path  = '/home/ec2-user/test'

# Choose different data source, you could choose following: 'FTP', 'URL_JSON'
	data_source = 'URL_JSON'


# if you choose 'URL_JSON', adjust parameters below:
	url = 'URL'

# if you choose 'FTP', adjust parameters below:
	ftp_site = 'Dummy'
	ftp_user = 'Dummy'
	ftp_pw = 'Dummy'
	ftp_path = 'Dummy'


###############################################################################################
#################################    Make Change Above        #################################
###############################################################################################


	main(
		aws_config_file,
		data_source,
		url,
		ftp_site,
		ftp_user,
		ftp_pw,
		ftp_path,
		local_path
		)



	sys.stdout = orig_stdout
	logs.close()




	