#!/usr/bin/env python
# coding: utf-8

# # GOAL
# Consume talentlms apis and save data to gcs:
# - Basic data
#     - Call branches api to get branches master data;
#     - Persist branches masterdata;
#     - Call users api without user id to get users master data;
#     - Persist user masterdata;
#     - Call courses api without course_id to get courses masterdata;
#     - Persist courses master data;
#     - Call groups api to get groups master data;
#     - Persist group masterdata;
# - Detailed data
#     - For each active user, call users api for the user_id to get badges, certifications, branches, groups and courses for the user;
#     - Persist badges, certifications, branches, groups and courses for the user;
#     - For each course in api response, call course api with course_id unit;
#     - Persist units by course as course masterdata;
#     - For each unit in each course, call unit api to get user data for each unit;
#     - Persist user data by unit in course;
#     - For each unit that is a test, call test api per enrolled user to get user responses;
#     - Persist user response by test;
# 

# # PACKAGES

import gcsfs
import os
import talentlms
import yaml
import json
from datetime import date


# # PARAMETERS

# gcp
gcp_project = "analytics-dev-308300"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../keys/gcp_key.json"
folder_name = 'talentlms/' + date.today().strftime('%Y%m%d')

# talent lms
api_key = yaml.load(open("../keys/talentlms_api_key.yaml"), Loader=yaml.BaseLoader)
domain = "humane.talentlms.com"


# FUNCTIONS

def save2gcs(file_storage,file_path,api_result):
    """
    (obj,str,obj)
    Save api_result as json as a file in gcs under file_path using file_storage object.
    """
    with file_storage.open(file_path,mode='w') as f:
        f.write(json.dumps(api_result))

def call_detail_api(head_data,api_call):
    """
    (list of dicts,integer,method,integer)--> list of dicts
    """
    detail=[]
    for item in head_data:
        detail.append(api_call(item['id']))
    return detail

def call_detail_api_for_units(course_detail_data):
    """
    (list of dicts,integer,method,integer)--> list of dicts
    """
    detail=[]
    set_unit_id = set()
    for item in course_detail_data:
        for unit in item['units']:
            set_unit_id.add(unit['id'])
    for unit_id in set_unit_id:
        detail.append(lms.get_user_progress_in_units(unit_id=unit_id))
    return detail


# SCRIPT

# open gcs file
fs = gcsfs.GCSFileSystem(project=gcp_project, access="read_write")
# create folder in gcs if it does not already exist
try:
    fs.mkdir('talentlms')
except:
    print('bucket already exists')

# instantiate talentlms api connector
lms = talentlms.api(domain, api_key["api_key"])

# call apis
api_data = {
    'branches':lms.branches(),
    'users':lms.users(),
    'courses':lms.courses(),
    'groups':lms.groups(),
    }
api_data['users_detail'] = call_detail_api(api_data['users'],lms.users)
api_data['courses_detail'] = call_detail_api(api_data['courses'],lms.courses)
api_data['units_detail'] = call_detail_api_for_units(api_data['courses_detail'])

# save data to gcs
for api in api_data:
    save2gcs(fs, folder_name+'/'+api+'.json', api_data[api])