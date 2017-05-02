
# encoding = utf-8

import os
import sys
import time
import datetime
import json
import kvstore_lib
#import simple_salesforce
from salesforce_reporting import Connection, ReportParser

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
    helper.log_info("report=" + json.dumps(inputname) + " message='collecting salesforce report'")

    #Set module level variables from input arguments
    opt_username = helper.get_arg('username')
    opt_password = helper.get_arg('password')
    opt_token = helper.get_arg('security_token')
    opt_kvstore = helper.get_arg('kvstore') or None
    opt_report = helper.get_arg('report_id') or None
    enable_index = helper.get_arg('enable_indexing')
    
    #Set module level kvstore variables
    enable_lookup = helper.get_arg('enable_lookup_configuration')
    enable_kv = helper.get_arg('enable_kvstore')
    enable_purge =  helper.get_arg('enable_purge')
    opt_record_key = helper.get_arg('kvstore_key') or None
    
    #Log proxy configuration
    #if opt_proxy == None:
    #    helper.log_info("No proxy settings enabled.")
    #else:
    #    helper.log_debug("Proxy settings: " + json.dumps(opt_proxy))
    
    try:    
        sf = Connection(username=opt_username, password=opt_password, security_token=opt_token, proxies=opt_proxy)
        report = sf.get_report(opt_report)
        parser = ReportParser(report)
    except Exception as salesforce_error:
        #Save to input checkpoint
        con_key = inputname + "-" + opt_report
        con_state = {}
        con_state['report_name'] = inputname
        con_state['report_id'] = opt_report
        con_state['status'] = "failure"
        con_state['kvstore'] = opt_kvstore or None
        con_state['_updated'] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) #Add meta value for troubleshooting
        con_state['message'] = "Salesforce connection error: see logs for more detail"
        helper.save_check_point(con_key, con_state)
        helper.log_debug("report=" + inputname + " message='checkpointing salesforce error state' checkpoint_key='" + con_key + "' checkpoint_state='" + json.dumps(con_state) + "'")
        raise salesforce_error
    
    #Configure KVStore if enabled
    if enable_kv is True:
        
        #Create base kvstore connectivity 
        kvs = kvstore_lib.KVClient(session['server_uri'], session['session_key'], helper.service)

        #Set KVStore to write events if not configured to input name else report_id
        if opt_kvstore is None:
            opt_kvstore = inputname or opt_report
            helper.log_debug("report=" + inputname + " message='kvstore name configured' opt_kvstore=" + opt_kvstore)

        #Logic for kvstore_key (inputs comma separated values to a list)
        if opt_record_key is not None:
            opt_record_key = opt_record_key.replace('"','').split(',')
            helper.log_debug("report=" + inputname + " message='kvstore key configured' opt_record_key=" + json.dumps(opt_record_key))

        #Create KVStore in specified app
        kvs_create = kvs.create_collection(collection=opt_kvstore, app=app_name)
        helper.log_info("report=" + inputname + " kvstore=" + opt_kvstore + " message='kvstore already present or has been created'")
    
        #Purge all existing data from KVStore
        if enable_purge is True:
            kvs_purge = kvs.delete_collection_data(collection=opt_kvstore, key_id=None, app=app_name)
            helper.log_info("report=" + inputname + " kvstore=" + opt_kvstore + " message='kvstore data purged'")
        
        #Create variables for lookup creation
        if enable_lookup is True:
            lookup_name = opt_kvstore
            kv_fields_list = [] #List of fields returned by API formatted for kvstore config
            lookup_fields_list = [] #List of fields returned by API formatted for lookup config
        
        #Configure KVStore fields for via REST API
        columns = parser.data["reportMetadata"]["detailColumns"]
        column_details = parser.data["reportExtendedMetadata"]["detailColumnInfo"]
        for key, value in enumerate(columns):
            lookup_key = column_details[value]["label"]
            key = "field." + lookup_key
            value = column_details[value]["dataType"]
            
            #Map Salesforce dataTypes to valid Splunk KVStore types
            if value == "date":
                value = "string"
            elif value == "int":
                value = "number"
            elif value == "currency":
                value = "number"
            elif value == "boolean":
                value = "bool"
            else:
                if value != ["array","number","bool","string","cidr","time"]:
                    value = "string"
            
            #Configure KVStore field object to POST to KVStore
            field = [(key, value)]
            add_field = kvs.config_collection(collection=opt_kvstore, app=app_name, data=field)
            #helper.log_debug("report=" + inputname + " kvstore=" + opt_kvstore + " message='Configuring kvstore field' field=" + json.dumps(field))
            
            #Configure fields to POST for lookup creation
            lookup_fields_list.append(lookup_key)
            kv_fields_list.append(lookup_key)
            
        #Log KVStore fields configured
        kvstore_fields = ",".join(['"{}"'.format(x) for x in kv_fields_list])
        helper.log_info("report=" + inputname + " kvstore=" + opt_kvstore + " message='Configured kvstore fields' count=" + str(len(lookup_fields_list)))
        helper.log_debug("report=" + inputname + " kvstore=" + opt_kvstore + " message='Configured lookup fields' fields='" + kvstore_fields + "'")

        #Create lookup configuration with fields returned
        if enable_lookup is True:
            lookup_fields = ",".join(['"{}"'.format(x) for x in lookup_fields_list])
            create_lookup = kvs.config_lookup(lookup=lookup_name, app=app_name, fields=lookup_fields)
            helper.log_info("report=" + inputname + " lookup_name=" + lookup_name + " message='Configured lookup fields' count=" + str(len(lookup_fields_list)))
            helper.log_debug("report=" + inputname + " lookup_name=" + lookup_name + " message='Configured lookup fields' fields='" + lookup_fields + "'")
            
    
    #Create proxy configuration to pass to request modules or set none
    #if helper.get_global_setting('proxy_enabled'):
    #    opt_proxy = helper.get_proxy()
    #    helper.log_info("[" + helper.input_type + "] Proxy settings enabled.")
    #else:
    #    opt_proxy = None
    #    helper.log_info("[" + helper.input_type + "] Proxy settings not enabled.")

    i=0 #iterator for indexed records processed
    k=0 #iterator for kvstore records processed
    u=0 #iterator for kvstore records updated
    for record in parser.kvstore_dict():

        #Function: Save to input checkpoint
        def cpt(status, message):
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
            helper.log_debug("report=" + inputname + " message='checkpointing states' checkpoint_key='" + cpt_key + "' checkpoint_state='" + json.dumps(cpt_state) + "'")
        
        
        #Checks if indexing enabled and writes events to specified index
        if enable_index is True:
            event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(record))
            ew.write_event(event)
            i = i + 1 #Count records indexed
            helper.log_debug("report=" + inputname + " message='indexed events' status=successful event_count=" + str(i))
            
        #Checks if kvstore is enabled and writes events based on kvstore settings
        if enable_kv is True:
            #Create timestamp for kvstore metadata
            time_updated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            
            #Check for record_id field(s) in configuration & build _key
            if opt_record_key is None:
                record['_updated'] = time_updated #Add meta value for troubleshooting
                
                insert_new = kvs.insert_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", data=record)
                k = k + 1 #Count records stored to kvstore
                helper.log_debug("report=" + inputname + " kvstore=" + opt_kvstore + " message='record inserted in kvstore' status=successful event_count=" + str(k))
                
            elif opt_record_key is not None:
                #Join record_id with "-" as key for record
                key = str("-".join([record[x] for x in opt_record_key]))
                helper.log_debug("report=" + inputname + " kvstore=" + opt_kvstore + " message='created record_id' key_id=" + key + " event_count=" + str(k))

                #If purge is enabled, insert of records is all that is allowed
                if enable_purge is True:
                    record['_updated'] = time_updated #Add meta value for troubleshooting
                    record['_key'] = key
                    insert_purge = kvs.insert_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", data=record)
                    k = k + 1 #Count records stored to kvstore
                    helper.log_debug("report=" + inputname + " kvstore=" + opt_kvstore + " message='kvstore purged & record inserted in kvstore' status=successful event_count=" + str(k))
                
                #Attempt to update existing records else insert into kvstore
                elif enable_purge is not True:
                    try:
                        record['_updated'] = time_updated #Add meta value for troubleshooting
                        record['_key'] = key
                        update = kvs.update_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", key_id=key, data=record)
                        k = k + 1 #Count records stored to kvstore
                        u = u + 1 #Count records stored to kvstore
                        helper.log_debug("report=" + inputname + " kvstore=" + opt_kvstore + " message='record updated in kvstore' status=successful key=" + key)
                    except:
                        record['_updated'] = time_updated #Add meta value for troubleshooting
                        record['_key'] = key
                        update_new = kvs.insert_collection_data(collection=opt_kvstore, app=app_name, owner="nobody", data=record)
                        k = k + 1 #Count records stored to kvstore
                        helper.log_debug("report=" + inputname + " kvstore=" + opt_kvstore + " message='record inserted in kvstore' status=successful key=" + key)
            else:
                #Log failure to insert into kvstore
                cpt("failed","unable to write record to kvstore")
                helper.log_error("report=" + inputname + " kvstore=" + opt_kvstore + " message='unable to write record to kvstorestatu=failed record=" + json.dumps(record))

    #Save to input checkpoint
    end_key = inputname + "-" + opt_report
    end_state = {}
    end_state['report_name'] = inputname
    end_state['report_id'] = opt_report
    end_state['status'] = "success"
    end_state['kvstore'] = opt_kvstore or None
    end_state['_updated'] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) #Add meta value for troubleshooting
    end_state['message'] = "report collection complete"
    helper.save_check_point(end_key, end_state)
    helper.log_debug("report=" + inputname + " message='report collection complete' checkpoint_key='" + end_key + "' checkpoint_state='" +     json.dumps(end_state) + "'")
    
    #Log completion
    helper.log_info("report=" + inputname + " message='report collection complete' indexed=" + str(i) + " kvstore=" + str(k) + " updated=" + str(u))