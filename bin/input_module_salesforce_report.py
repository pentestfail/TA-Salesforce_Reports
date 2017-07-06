
# encoding = utf-8

import os
import sys
import time
import datetime
import json
import csv
import kvstore_lib
from simple_salesforce import Salesforce
#from salesforce_reporting import Connection, ReportParser

def validate_input(helper, definition):
    username = definition.parameters.get('username', None)
    password = definition.parameters.get('password', None)
    security_token = definition.parameters.get('security_token', None)
    report_id = definition.parameters.get('report_id', None)
    kvstore = definition.parameters.get('kvstore', None)
    kvstore_key = definition.parameters.get('kvstore_key', None)
    pass

def collect_events(helper, ew):
    #Set base variables
    app_name =  helper.get_app_name()
    opt_proxy = helper.get_proxy() or None
    session = helper.context_meta
    inputname = helper.get_input_stanza_names()
    inputsource = helper.get_input_type() + ":" + inputname
    helper.log_info("input_type=salesforce_reports input={0:s} message='Collecting salesforce report.'".format(inputname))

    #Set module level variables from input arguments
    opt_username = helper.get_arg('username')
    opt_password = helper.get_arg('password')
    opt_token = helper.get_arg('security_token')
    opt_kvstore = helper.get_arg('kvstore') or None
    opt_report = helper.get_arg('report_id')
    enable_index = helper.get_arg('enable_indexing')
    
    #Set module level kvstore variables
    enable_lookup = helper.get_arg('enable_lookup_configuration')
    enable_kv = helper.get_arg('enable_kvstore')
    enable_purge =  helper.get_arg('enable_purge')
    opt_record_key = helper.get_arg('kvstore_key') or None
    
    #Function: Save to input checkpoint
    def cpt(status, message, time_updated=None, i=None, k=None):
        cpt_key = inputname + "-" + opt_report
        cpt_state = {}
        cpt_state['report_name'] = inputname
        cpt_state['report_id'] = opt_report
        cpt_state['status'] = status
        cpt_state['kvstore'] = opt_kvstore or None
        cpt_state['_updated'] = time_updated or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) #Add meta value for troubleshooting
        cpt_state['records_index'] = str(i) or None  #Logs number of records indexed
        cpt_state['records_kvstore'] = str(k) or None #Logs number of records sent to kvstore
        cpt_state['message'] = message
        helper.save_check_point(cpt_key, cpt_state)
        helper.log_debug("input_type=salesforce_reports input={0:s} message='checkpointing states' checkpoint_key='{1:s}' checkpoint_state='{2:s}'".format(inputname,cpt_key,cpt_state))

    try:
        #Retrieves report metadata for formatting & validation
        sf = Salesforce(username=opt_username, password=opt_password, security_token=opt_token, proxies=opt_proxy)
        sf_describe = helper.send_http_request("https://{0}/services/data/v39.0/analytics/reports/{1}/describe".format(sf.sf_instance, opt_report), method="GET", headers=sf.headers, cookies={'sid': sf.session_id}, parameters=None, payload=None, verify=True, cert=None, timeout=25, use_proxy=True)
        sf_describe_data = json.loads(sf_describe.content)

        #Pulls CSV from Salesforce via export
        sf_csv = helper.send_http_request("https://{0}/{1}?view=d&snip&export=1&enc=UTF-8&xf=csv".format(sf.sf_instance, opt_report), method="GET", headers=sf.headers, cookies={'sid': sf.session_id}, parameters=None, payload=None, verify=True, cert=None, timeout=120, use_proxy=True)
        sf_csv_raw = sf_csv.content.decode('utf8')
        sf_csv_data = list(csv.DictReader(sf_csv_raw.split('\n')))
    except Exception as salesforce_error:
        #Save to input status checkpoint
        cpt("failure", "Salesforce connection error: see logs for more detail")
        helper.log_error("input_type=salesforce_reports input={0:s} message='Salesforce connection error.'".format(inputname))
        raise salesforce_error

    #Create timestamp for kvstore metadata
    time_updated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    i=0 #iterator for indexed records processed
    k=0 #iterator for kvstore records processed
    u=0 #iterator for kvstore records updated
    
    #Checks if indexing enabled and writes events to specified index
    if enable_index is True:
        for record in sf_csv_data[:-5]:
            ew.write_event(helper.new_event(source=inputsource, index=helper.get_output_index(), sourcetype=helper    .get_sourcetype(), data=json.dumps(record)))
            i += 1 #Count records indexed
            helper.log_debug("input_type=salesforce_reports input={0:s} message='indexed events' status=successful event_count={1:d}".format(inputname,i))

    #Configure KVStore if enabled
    if enable_kv is True:
        #Create base kvstore connectivity 
        kvs = kvstore_lib.KVClient(session['server_uri'], session['session_key'], helper.service)
        input_id = inputname + "_" + opt_report
    
        #Set KVStore to write events if not configured to input name else report_id
        if opt_kvstore is None:
            opt_kvstore = inputname or opt_report
            helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='kvstore name configured'".format(inputname,opt_kvstore))

        #Logic for kvstore_key (inputs comma separated values to a list)
        if opt_record_key is not None:
            opt_record_key = opt_record_key.replace('"','').split(',')
            helper.log_debug("input_type=salesforce_reports input={0:s} message='kvstore key configured' opt_record_key='{1}'".format(inputname,json.dumps(opt_record_key)))
    
        #Create KVStore in specified app
        kvs_create = kvs.create_collection(collection=opt_kvstore, app=app_name)
        helper.log_info("input_type=salesforce_reports input={0:s} kvstore={1:s} message='kvstore already present or has been created'".format(inputname,opt_kvstore))
        
        #Purge all existing data from KVStore
        if enable_purge == 'all':
            kvs_purge = kvs.delete_collection_data(collection=opt_kvstore, key_id=None, app=app_name)
            helper.log_info("input_type=salesforce_reports input={0:s} kvstore={1:s} message='all kvstore data purged'".format(inputname,opt_kvstore))
                
        #Purge this report's records existing from KVStore
        if enable_purge == 'report':
            records_query = '{"_input_id":"' + str(input_id) + '"}'
            kvs_purge = kvs.delete_collection_query(collection=opt_kvstore, query=str(records_query), app=app_name)
            helper.log_info("input_type=salesforce_reports input={0:s} kvstore={1:s} message='input records data purged from kvstore'".format(inputname,opt_kvstore))

        #Create variables for lookup creation
        if enable_lookup is True:
            lookup_name = opt_kvstore
            kv_fields_list = [] #List of fields returned by API formatted for kvstore config
            lookup_fields_list = [] #List of fields returned by API formatted for lookup config
        
            #Configure KVStore fields for via REST API
            columns = sf_describe_data["reportMetadata"]["detailColumns"]
            column_details = sf_describe_data["reportExtendedMetadata"]["detailColumnInfo"]
            for key, value in enumerate(columns):
                lookup_key = column_details[value]["label"] #.replace(" ", "_")
                field_key = "field." + lookup_key
                field_value = column_details[value]["dataType"]
                
                #Map Salesforce dataTypes to valid Splunk KVStore types
                if field_value == "date":
                    field_value = "string"
                elif field_value == "int":
                    field_value = "number"
                elif field_value == "currency":
                    field_value = "number"
                elif field_value == "boolean":
                    field_value = "bool"
                else:
                    if field_value != ["array","number","bool","string","cidr","time"]:
                        field_value = "string"
                
                #Configure KVStore field object to POST to KVStore
                field = [(field_key, field_value)]
                add_field = kvs.config_collection(collection=opt_kvstore, app=app_name, data=field)
                helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='Configuring kvstore field' field={2}".format(inputname,opt_kvstore,json.dumps(field)))
                
                #Configure fields to POST for lookup creation
                lookup_fields_list.append(lookup_key)
                kv_fields_list.append(lookup_key)
                
            #Create kvstore configuration with fields returned
            kvstore_fields = ",".join(['"{}"'.format(x) for x in kv_fields_list])
            helper.log_info("input_type=salesforce_reports input={0:s} kvstore={1:s} message='Configured kvstore fields' count={2:d}".format(inputname,opt_kvstore,len(kv_fields_list)))
            helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='Configured kvstore fields' fields='{2}'".format(inputname,opt_kvstore,json.dumps(kv_fields_list)))
    
            #Create lookup configuration with fields returned
            lookup_fields = ",".join(['"{}"'.format(x) for x in lookup_fields_list])
            create_lookup = kvs.config_lookup(lookup=lookup_name, app=app_name, fields=lookup_fields)
            helper.log_info("input_type=salesforce_reports input={0:s} lookup_name={1:s} message='Configured lookup fields' count={2:d}".format(inputname,lookup_name,len(lookup_fields_list)))
            helper.log_debug("input_type=salesforce_reports input={0:s} lookup_name={1:s} message='Configured lookup fields' fields='{2}'".format(inputname,lookup_name,lookup_fields))

        #Writes events based on kvstore settings
        # for record in sf_csv_data:
        for record in sf_csv_data[:-5]: #Drops Salesforce footer appended to end of report
            record['_updated'] = time_updated #Add meta value for troubleshooting
            record['_input_id'] = input_id #Add meta value for purge options
            
            #Check for record_id field(s) in configuration & build _key
            if opt_record_key is None:
                insert_new = kvs.insert_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", data=record)
                k += 1 #Count records stored to kvstore
                helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='record inserted in kvstore' status=successful event_count={2:d}".format(inputname,opt_kvstore,k))
                
            elif opt_record_key is not None:
                #Join record_id with "-" as key for record
                record_key = str("-".join([record[x] for x in opt_record_key]))
                record['_key'] = record_key #Set _key required for functions
                
                #If purge all is enabled, insert of records is all that is allowed
                if enable_purge == 'all':
                    insert_purge = kvs.insert_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", data=record)
                    k += 1 #Count records stored to kvstore
                    helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='record inserted in kvstore' status=successful event_count={2:d} key={3:s}".format(inputname,opt_kvstore,k,record_key))
                
                #Attempt to update existing records else insert into kvstore
                else:
                    try:
                        update = kvs.update_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", key_id=record_key, data=record)
                        k += 1 #Count records stored to kvstore
                        u += 1 #Count records stored to kvstore
                        helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='record updated in kvstore' status=successful event_count={2:d} key={3:s}".format(inputname,opt_kvstore,u,record_key))
                    except:
                        update_new = kvs.insert_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", data=record)
                        k += 1 #Count records stored to kvstore
                        helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='record inserted in kvstore' status=successful event_count={2:d} key={3:s}".format(inputname,opt_kvstore,k,record_key))
            else:
                #Log failure to insert into kvstore
                cpt("failed","unable to write record to kvstore")
                helper.log_debug("input_type=salesforce_reports input={0:s} kvstore={1:s} message='unable to write record to kvstore.' status=failed event_count={2:d} key={3:s} record='{4}'".format(inputname,opt_kvstore,record_key,record))

    #Save to input checkpoint
    cpt("success", "report collection completed", time_updated, i, k)

    #Warn if total report records equal max of API (2,000 rows)
    #if [i,k,u] == 2000:
    #    helper.log_warning("input_type=salesforce_reports input={0:s} message='report may be truncated due to Salesforce API limit of 2,000 rows' indexed={1:d} kvstore={2:d} updated={3:d}".format(inputname,i,k,u))

    #Log completion
    helper.log_info("input_type=salesforce_reports input={0:s} message='report collection complete' indexed={1:d} kvstore={2:d} updated={3:d}".format(inputname,i,k,u))
