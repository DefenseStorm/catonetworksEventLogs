#!/usr/bin/env python3

import sys,os,getopt
import traceback
import os
import fcntl
import json
from urllib.request import urlopen, URLError, HTTPError
from zipfile import ZipFile
from io import StringIO
from datetime import datetime

sys.path.insert(0, './ds-integration')
from DefenseStorm import DefenseStorm

num_failures = 5

class integration(object):

    JSON_field_mappings = {
            'creationTime': 'timestamp',
            'destinationIp': 'ip_dest',
            'clientIp': 'client_ip',
            'sourceIp': 'ip_src',
            'server_port': 'service_port',
            'prettyType': 'short_message',
            'sourceCountry': 'src_location',
            'destinationCountry': 'dest_location',
            'destinationName': 'dest_host',
    }


    def convertTime(self, mystring):
        if "||" in mystring and "rt=" in mystring:
            start = mystring.find("rt=")
            timestring = mystring[start + 3:start + 31]
            dt = datetime.strptime(timestring, "%a %b %d %H:%M:%S %Z %Y")
            epoch = (dt - datetime(1970,1,1)).total_seconds()
            newline = mystring[:start+3] + "%d" %epoch + mystring[start + 31:]
            return newline
        else:
            return mystring


    def get_logs(self):
        bucket = self.ds.config_get('cato', 'BUCKET')
        api_key = self.ds.config_get('cato', 'API_KEY')
        start_index = self.ds.config_get('cato', 'START_INDEX')
        self.state_dir = os.path.join(self.ds.config_get('cato', 'APP_PATH'), 'state')
        mystate = self.ds.get_state(self.state_dir)

        if mystate == None:
            mystate = {}
            mystate['last'] = int(start_index)
            mystate['count'] = 0

        while True:
            filename = "CATO" + '{:020d}'.format(mystate['last']) + ".zip"
        
            url = "%s/%s/%s" %(bucket, api_key, filename)
            try:
                f = urlopen(url, timeout = 10)
                with open(filename, "wb") as local_file:
                    local_file.write(f.read())
            except HTTPError as e:
                if e.code == 403:
                    self.ds.logger.info('File %s not available. No files to process or issue with START_INDEX' %filename)
                    break
            except URLError as e:
                self.ds.logger.errro('%s %s' %(e.reason, url))
                pass
            except Exception as e:
                self.ds.logger.error('%s %s' %(e.reason, url))
                return
   
            retcode = f.getcode()
            if retcode == 200:
                self.ds.logger.info('Processing file %s.' %filename)
                self.ds.set_state(self.state_dir, mystate)
                try:
                    zipfile = ZipFile(filename)
                    zipfile.setpassword(api_key[:10].encode('utf-8'))
                    infolist = zipfile.infolist()
                    event_list = []
                    for item in infolist:
                        foofile = zipfile.open(item)
                        event_list += foofile.readlines()
                    os.remove(filename)
                    for bline in event_list:
                        line = bline.decode('utf-8')
                        line = self.convertTime(line)
                        if "||" in line:
                            self.ds.writeEvent(line.replace('||', '|CatoNetworks|'))
                        else:
                            json_event = json.loads(line)
                            if 'prettyType' in json_event.keys():
                                json_event['message'] = json_event['prettyType']
                            else:
                                json_event['message'] = 'NoType'
                            if 'sourceIp' in json_event.keys():
                                json_event['message'] = json_event['message'] + ' from ' + json_event['sourceIp']
                            if 'destinationIp' in json_event.keys():
                                json_event['message'] = json_event['message'] + ' to ' + json_event['destinationIp']

                            self.ds.writeJSONEvent(json_event, JSON_field_mappings = self.JSON_field_mappings)
                except Exception as e:
                    traceback.print_exc()
                    self.ds.logger.error('Processing file %s.  Error: %s' %(filename, str(e)))
                    mystate['last'] += 1
                    mystate['count'] = 1
                    self.ds.set_state(self.state_dir, mystate)
                    return
                mystate['last'] += 1
                mystate['count'] = 1
                self.ds.set_state(self.state_dir, mystate)

            elif mystate['count'] > num_failures:
                self.ds.logger.error('Too many failures for file %s. Skipping.' %filename)
                mystate['last'] += 1
                mystate['count'] = 0
                self.ds.set_state(self.state_dir, mystate)
            else:
                mystate['count'] += 1
                self.ds.set_state(self.state_dir, mystate)


    def run(self):
        try:
            pid_file = self.ds.config_get('cato', 'pid_file')
            fp = open(pid_file, 'w')
            try:
                fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                self.ds.logger.error("An instance of catonetworksEventLogs is already running")
                # another instance is running
                sys.exit(0)
            self.ds.logger.info('Getting logs')
            self.get_logs()
        except Exception as e:
            traceback.print_exc()
            self.ds.logger.error("Exception {0}".format(str(e)))
            return

    def usage(self):
        print('')
        print(os.path.basename(__file__))
        print('')
        print('  No Options: Run a normal cycle')
        print('')
        print('  -t    Testing mode.  Do all the work but do not send events to GRID via ')
        print('        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\'')
        print('        in the current directory')
        print('')
        print('  -l    Log to stdout instead of syslog Local6')
        print('')
    
    def __init__(self, argv):

        self.testing = False
        self.send_syslog = True
        self.ds = None
    
        try:
            opts, args = getopt.getopt(argv,"htnld:",["datedir="])
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                self.usage()
                sys.exit()
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-l"):
                self.send_syslog = False
    
        try:
            self.ds = DefenseStorm('catonetworksEventLogs', testing=self.testing, send_syslog = self.send_syslog)
        except Exception as e:
            traceback.print_exc()
            try:
                self.ds.logger.error('ERROR: ' + str(e))
            except:
                pass


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
