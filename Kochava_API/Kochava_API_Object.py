# -*- coding: utf-8 -*-
"""
Created on 5/25/2016
Kochava API object 

@author: bowei.zhang

Usage:
    start_date = "01/01/2016"    # Format: MM/DD/YYYY
    end_date = "02/13/2016"      # Data do not include the end_date

# Local path for temp files storage
    local_path = 'C:\\Users\\'

# Which app you want to pull    
    app_id_list = ['ID']  

    agent = Kochava_API() 

    agent.set_local_path(local_path)
    for app_id in app_id_list:
        agent.set_app_id(app_id)
        agent.get_app_event(start_date,end_date)


"""

import json
import datetime
import time 
import logging
import requests
import csv
import sys
import os


class Kochava_API(object):
    ''' http://support.kochava.com/support/solutions/articles/1000071256-json-reporting-api-methods#evedat'''
    def __init__(self,api_key = 'E47B4E87-DAC6-4653-8543-9C3143BA5C60',account_name='nbc universal'):
        ''' Initialize the API Agent with API KEY and Account Name'''        
        self.api_key = api_key        
        self.account_name = account_name
        self.app_id = None


    def read_url(self,url,post_body):
        ''' read url data '''
        try:
            r = requests.post(url, data=post_body)
            response = r.json()
 

        except Exception as e:
            print('Had trouble read API URL\n')
            print logging.warning(e)

        return(response)


#    def get_app_summary(self,summary_post_body):
#        ''' Get app summary data '''
#        app_summary_url = 'https://reporting.api.kochava.com/summary' 
    #    summary_post_body = self.get_post_body(summary_post_body)
  #      app_summary_raw = self.read_url(app_summary_url,summary_post_body)
#        with open('data.json', 'w') as outfile:
 #           json.dump(app_summary_raw, outfile)
 #       app_summary = self.json_to_pd(app_summary_raw)
   #     return(app_summary)         
        


    def get_app_event(self,start_date,end_date):
        ''' Get app event data '''
        app_event_url = 'https://reporting.api.kochava.com/detail'
	#app_event_url = 'https://reporting.api.kochava.com/v1.1/detail'

        epoch_start_date = self.date_to_epoch(start_date)
        epoch_end_date = self.date_to_epoch(end_date)


        event_post_body = """{
              "api_key": "%s",
              "app_guid": "%s",
              "time_start": "%d",
              "time_end": "%d",
              "traffic": [

                           "event"

              ],

              "delivery_method": [
                           "S3link"
              ],
              "delivery_format": "csv",
              "notify": [
                           "bowei.zhang@360i.com"
              ]
        }""" % (self.api_key,self.app_id,epoch_start_date,epoch_end_date)

        app_event_raw = self.read_url(app_event_url,event_post_body)
        with open('data.json', 'w') as outfile:
            json.dump(app_event_raw, outfile)


        success = False
        if app_event_raw["status"] == "queued":
            success = True
            report_token = app_event_raw["report_token"] 
            self.get_file(report_token,start_date,end_date)
        else:
            success = False
            print ('Had trouble connect through API\n')
            sys.exit()



    def get_app_install(self,start_date,end_date):
        ''' Get app install data '''
        app_install_url = 'https://reporting.api.kochava.com/detail'
    #app_event_url = 'https://reporting.api.kochava.com/v1.1/detail'

        epoch_start_date = self.date_to_epoch(start_date)
        epoch_end_date = self.date_to_epoch(end_date)


        install_post_body = """{
              "api_key": "%s",
              "app_guid": "%s",
              "time_start": "%d",
              "time_end": "%d",
              "traffic": [

                           "install"

              ],

              "delivery_method": [
                           "S3link"
              ],
              "delivery_format": "csv",
              "notify": [
                           "bowei.zhang@360i.com"
              ]
        }""" % (self.api_key,self.app_id,epoch_start_date,epoch_end_date)

        app_install_raw = self.read_url(app_install_url,install_post_body)
        with open('install_data.json', 'w') as outfile:
            json.dump(app_install_raw, outfile)


        success = False
        if app_install_raw["status"] == "queued":
            success = True
            report_token = app_install_raw["report_token"] 
            self.get_install_file(report_token,start_date,end_date)
        else:
            success = False
            print ('Had trouble connect through API\n')
            sys.exit()



    def set_app_id(self,app_id):
        ''' set the app id for the API agent'''
        self.app_id = app_id

    def set_local_path(self,local_path):
        ''' set the local_path for files temprory storage'''
        self.local_path = local_path

    def set_install_local_path(self,install_local_path):
        ''' set the install_local_path for installfiles temprory storage'''
        self.install_local_path = install_local_path

    def date_to_epoch(self,date):
        ''' convert date to epoch time format that needed for API call'''
        pattern = '%m/%d/%Y'
        epoch = int(time.mktime(time.strptime(date, pattern)))
        return epoch

    def get_file(self,report_token,start_date,end_date):
        ''' Download report to local'''


        report_status_url = 'https://reporting.api.kochava.com/progress'
	#report_status_url = 'https://reporting.api.kochava.com/v1.1/progress'

        status_post_body = """{
        "api_key": "%s",
        "app_guid": "%s",
        "token": "%s"
        }""" % (self.api_key,self.app_id,report_token)

        report_status_raw = self.read_url(report_status_url,status_post_body)


        i = 0      # Add a imit times that make a status API call in order to avoid infinite while loop
        while report_status_raw["status"] != "completed" and i <= 10000:
            time.sleep(5)
            i = i + 1
            print i
            report_status_raw = self.read_url(report_status_url,status_post_body)

        j = i*5
        print '\n' + 'It takes ',j,' seconds to download files into local folder'
        download_url = report_status_raw["report"]

        with requests.Session() as s:
            download = s.get(download_url)
            filename = '%s_%s_%s.csv' %(start_date.replace('/',''),end_date.replace('/',''),self.app_id)
            with open(os.path.join(self.local_path, filename), 'wb') as f:
                f.write(download.content)
                f.close()

        self.rm_comma_to_pipe(self.local_path,filename,9)
        self.rm_comma_to_pipe(self.local_path,filename,20)
        self.rm_comma_to_pipe(self.local_path,filename,39)

    def get_install_file(self,report_token,start_date,end_date):
        ''' Download install report to local'''


        report_status_url = 'https://reporting.api.kochava.com/progress'
    #report_status_url = 'https://reporting.api.kochava.com/v1.1/progress'

        status_post_body = """{
        "api_key": "%s",
        "app_guid": "%s",
        "token": "%s"
        }""" % (self.api_key,self.app_id,report_token)

        report_status_raw = self.read_url(report_status_url,status_post_body)


        i = 0      # Add a imit times that make a status API call in order to avoid infinite while loop
        while report_status_raw["status"] != "completed" and i <= 10000:
            time.sleep(5)
            i = i + 1
            print i
            report_status_raw = self.read_url(report_status_url,status_post_body)

        j = i*5
        print '\n' + 'It takes ',j,' seconds to download install files into local folder'
        download_url = report_status_raw["report"]

        with requests.Session() as s:
            download = s.get(download_url)
            filename = 'install_%s_%s_%s.csv' %(start_date.replace('/',''),end_date.replace('/',''),self.app_id)
            with open(os.path.join(self.install_local_path, filename), 'wb') as f:
                f.write(download.content)
                f.close()

        self.rm_comma_to_pipe(self.install_local_path,filename,23) # install_devices_ids
        self.rm_comma_to_pipe(self.install_local_path,filename,36) # installgeo_country_name



    def rm_comma_to_pipe(self,path,filename,col_num):            #  Remove unwanted comma in CSV indrder to avoid future pragmatic error
        os.rename(os.path.join(path, filename),'rm_comma_to_pipe_temp.csv')     #  List index start from "0", so the first column' column number is 0, and so on
        with open('rm_comma_to_pipe_temp.csv') as fi:
            fo = open(os.path.join(path, filename), 'wb')
            reader = csv.reader(fi)
            writer = csv.writer(fo)
            for row in reader:
                row[col_num] = row[col_num].replace(',','|')
                writer.writerow(row)
        fi.close()
        fo.close()
        os.remove('rm_comma_to_pipe_temp.csv')




if __name__ == "__main__":
    pass


