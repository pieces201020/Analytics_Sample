import csv
import json
import glob
import logging
import sys
import os
import AWS_Object
import Kochava_API_Object

def main(*args):
# Make kochava API call and download files
    API = Kochava_API_Object.Kochava_API()
    AWS = AWS_Object.AWS(aws_config_file)

    API.set_local_path(local_path)
    API.set_install_local_path(install_local_path)

    for app_id in app_id_list:
        API.set_app_id(app_id)
        try:
            API.get_app_event(start_date,end_date)
        except Exception as e:
            print '\n'+"Something wrong when downloading files from Kochava API"
            print logging.warning(e)
            print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

        # For install        
        try:
            API.get_app_install(start_date,end_date)
        except Exception as e:
            print '\n'+"Something wrong when downloading install files from Kochava API"
            print logging.warning(e)
            print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" 

# Upload files from local to AWS S3 (also make copy in S3)

    if [os.path.basename(x) for x in glob.glob("%s/*.csv" % local_path)] == []:# os.path.basename is to get rid of path before filename
        print '\n'+"There is no file in local, please check"
        sys.exit(-1)
    else:
        for filename in [os.path.basename(x) for x in glob.glob("%s/*.csv" % local_path)]:
            try:
                AWS.upload2S3(filename,local_path)
            except Exception as e:
                print '\n'+"Something wrong when uploading data into S3"
                print logging.warning(e)
                print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    # For install
    if [os.path.basename(x) for x in glob.glob("%s/*.csv" % install_local_path)] == []:# os.path.basename is to get rid of path before filename
        print '\n'+"There is no install file in local, please check"
        sys.exit(-1)
    else:
        for filename in [os.path.basename(x) for x in glob.glob("%s/*.csv" % install_local_path)]:
            try:
                AWS.install_upload2S3(filename,install_local_path)
            except Exception as e:
                print '\n'+"Something wrong when uploading install data into S3"
                print logging.warning(e)
                print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"



# Copy file from S3 to Redshift
    dest_conn = AWS.conn_to_rs()        
    try:
        AWS.upsert_data(dest_conn)
    except Exception as e:
        print '\n'+"Something wrong when upserting data into Redshift"
        print logging.warning(e)
        print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    try:
        AWS.upsert_install_data(dest_conn)
    except Exception as e:
        print '\n'+"Something wrong when upserting install data into Redshift"
        print logging.warning(e)
        print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"





    dest_conn.close()
        
    # Delete file on S3 staging folder
    AWS.s3Delete()


if __name__ == "__main__":

###############################################################################################
#################################    Make Change Below        #################################
############################################################################################### 

    start_date = "07/18/2016"    # Format: MM/DD/YYYY
    end_date = "08/08/2016"      # Data do not include the end_date

# Local path for temp files storage
    local_path = '/home/ec2-user/nbcu/test'
    install_local_path = '/home/ec2-user/nbcu/install'

# Which app you want to pull    
    app_id_list = ['ID 1','ID 2']   

# The location where the AWS_config file is
    aws_config_file = 's3://config.json'

###############################################################################################
#################################    Make Change Above        #################################
###############################################################################################   

# logs
    logging.basicConfig(filename='Errors.txt',stream=sys.stdout,level=logging.DEBUG)
    orig_stdout = sys.stdout
    logs = file('Logs'+'.txt', 'w')    
    sys.stdout = logs

    main(start_date,
        end_date,
        local_path,
        install_local_path,
        app_id_list,
        aws_config_file)

    sys.stdout = orig_stdout
    logs.close()
    
