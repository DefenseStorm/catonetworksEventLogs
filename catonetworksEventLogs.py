#!/usr/bin/env python

import sys,os,getopt
import traceback
import os
from urllib2 import urlopen, URLError, HTTPError
from zipfile import ZipFile
from StringIO import StringIO
from datetime import datetime

sys.path.insert(0, './ds-integration')
from DefenseStorm import DefenseStorm

num_failures = 5

class integration(object):

    def convertTime(self, mystring):
        start = mystring.find("rt=")
        timestring = mystring[start + 3:start + 31]
        dt = datetime.strptime(timestring, "%a %b %d %H:%M:%S %Z %Y")
        epoch = (dt - datetime(1970,1,1)).total_seconds()
        newline = mystring[:start+3] + "%d" %epoch + mystring[start + 31:]
        return newline

    def get_logs(self):
        bucket = self.ds.config_get('cato', 'BUCKET')
        api_key = self.ds.config_get('cato', 'API_KEY')
        self.state_dir = os.path.join(self.ds.config_get('cato', 'APP_PATH'), 'state')
        mystate = self.ds.get_state(self.state_dir)

        if mystate == None:
            mystate = {}
            mystate['last'] = 0
            mystate['count'] = 0


        while True:
            filename = "CATO" + '{:020d}'.format(mystate['last']) + ".zip"
        
            url = "%s/%s/%s" %(bucket, api_key, filename)
            self.ds.log('DEBUG', "URL: %s" % url)
            try:
                f = urlopen(url)
                with open(filename, "wb") as local_file:
                    local_file.write(f.read())
            except HTTPError, e:
                self.ds.log('ERROR', '%s %s' %(e.code, url))
                return
            except URLError, e:
                self.ds.log('ERROR', '%s %s' %(e.reason, url))
                pass
            except Exception, e:
                self.ds.log('ERROR', '%s %s' %(e.reason, url))
                return
    
            if retcode == 200:
                mystate['last'] += 1
                mystate['count'] = 1
                self.ds.set_state(self.state_dir, mystate)
                try:
                    zipfile = ZipFile(filename)
                    zipfile.setpassword(api_key[:10])
                    infolist = zipfile.infolist()
                    event_list = []
                    for item in infolist:
                        foofile = zipfile.open(item)
                        event_list += foofile.readlines()
                    os.remove(filename)
                    for line in event_list:
                        line = self.convertTime(line)
                        self.ds.writeEvent(line.replace('||', '|CatoNetworks|'))
                except Exception as e:
                    traceback.print_exc()
                    self.ds.log('ERROR', 'Unzipping %s.  Check file contents for errors' %(filename))
                    return

            elif retcode == 403:
                filename = mystate['last'] + '.zip'
                url = "%s/%s/%s" %(bucket, api_key, filename)
                #self.ds.log('DEBUG', "URL: %s" % url)
                try:
                    f = urlopen(url)
                    with open(filename, "wb") as local_file:
                        local_file.write(f.read())
                except HTTPError, e:
                    self.ds.log('ERROR', '%s %s' %(e.code, url))
                    return
                except URLError, e:
                    self.ds.log('ERROR', '%s %s' %(e.reason, url))
                    pass
                retcode = f.getcode()
                if retcode == 200:
                    mystate['last'] += 1
                    mystate['count'] = 1
                    self.ds.set_state(self.state_dir, mystate)
                    try:
                        zipfile = ZipFile(filename)
                        zipfile.setpassword(api_key[:10])
                        infolist = zipfile.infolist()
                        event_list = []
                        for item in infolist:
                            foofile = zipfile.open(item)
                            event_list += foofile.readlines()
                        os.remove(filename)
                        for line in event_list:
                            line = self.convertTime(line)
                            self.ds.writeEvent(line.replace('||', '|CatoNetworks|'))
                    except Exception as e:
                        traceback.print_exc()
                        self.ds.log('ERROR', 'Unzipping %s' %(filename))
                else:
                    self.ds.log('ERROR', 'Unable to get any files')
                    break
            elif mystate['count'] > num_failures:
                mystate['last'] += 1
                mystate['count'] = 0
                self.ds.set_state(self.state_dir, mystate)
            else:
                mystate['count'] += 1
                self.ds.set_state(self.state_dir, mystate)


    def run(self):
        self.ds.log('INFO', 'Getting logs')
        self.get_logs()
        #self.ds.writeCEFEvent()
    
    def usage(self):
        print
        print os.path.basename(__file__)
        print
        print '  No Options: Run a normal cycle'
        print
        print '  -t    Testing mode.  Do all the work but do not send events to GRID via '
        print '        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\''
        print '        in the current directory'
        print
        print '  -l    Log to stdout instead of syslog Local6'
        print
    
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
        except Exception ,e:
            traceback.print_exc()
            try:
                self.ds.log('ERROR', 'ERROR: ' + str(e))
            except:
                pass


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
