'''
Created on Jan 30, 2013

@author: mheilman
'''

import cherrypy, os

from Region import Region
import threading
import time, uuid, requests, logging, logging.handlers, json
from os import listdir
from os.path import isfile, join
from Monitor import Monitor as MonitorWrapper
from cherrypy.process.plugins import Monitor
import cherrypy

import xml.etree.ElementTree as ET

class Slave:
    
    def __init__(self, conf):
        # create process blanks
        self.procs = {}
        self.loaded = False
        self.port = conf['port']
        if len(conf['regionPorts']) < len(conf['consolePorts']):
            numProcs = len(conf['regionPorts'])
        else:
            numProcs = len(conf['consolePorts'])
        self.key = uuid.uuid4()
        self.host = conf['host']
        self.frontendAddress = conf['webAddress']
        for i in range(numProcs):
            procName = "%d" % (conf['regionPorts'][i])
            self.procs[procName] = Region(
                                        conf['regionPorts'][i], 
                                        conf['consolePorts'][i], 
                                        procName, 
                                        conf['binDir'],
                                        conf['regionDir'],
                                        self.frontendAddress,
                                        conf['regionAddress'])
        
        #we can't start up without our master config
        while not self.loaded:
            try:
                self.loaded = self.loadRemoteConfig()
            except Exception, e:
                print "Error contacting MGM: %s" % e
                time.sleep(30)
    
        self.monitor = MonitorWrapper()
        
        Monitor(cherrypy.engine, self.updateStats, frequency=10).subscribe()
        cherrypy.engine.subscribe('stop',self.engineExit)
    
        print "node started"
    
    def engineExit(self):
        for proc in self.procs:
            self.procs[proc].terminate()
    
    def __del__(self):
        self.procs = None
        self.monTimer.cancel()
        print "node stopping"
    
    def loadRemoteConfig(self):
        #load additional config from master service
        url = "http://%s/server/dispatch/node" % (self.frontendAddress)
        r = requests.post(url, data={'host':self.host, 'port':self.port, 'key':self.key, 'slots': len(self.procs)}, verify=False)
        if not r.status_code == requests.codes.ok:
            raise Exception("Error contacting MGM at %s" % url)
        
        result = json.loads(r.content)
        
        if not result["Success"]:
            raise Exception("Error loading config: %s" % result["Message"])
                
        for region in result['Regions']:
            candidate = None
            for proc in self.procs:
                if not self.procs[proc].isRegistered:
                    candidate = self.procs[proc]
                    break
            if not candidate:
                print "ERROR: too many regions for number of slots"
            else:
                candidate.registerRegion(region['name'])
        return True

        
    def updateStats(self):
        self.monitor.updateStatistics()
        stats = {}
        stats['host'] = self.monitor.stats
        stats['slots'] = len(self.procs)
        stats['processes'] = []
        for proc in self.procs:
            if self.procs[proc].isRegistered:
                self.procs[proc].updateProcStats()
                p = {}
                p['name'] = self.procs[proc].name
                p['running'] = str(self.procs[proc].isRunning())
                p['stats'] = self.procs[proc].stats
                stats['processes'].append(p)
        
        url = "http://%s/server/dispatch/stats/%s" % (self.frontendAddress, self.host)
        r = requests.post(url, data={"json": json.dumps(stats)}, verify=False)
        if not r.status_code == requests.codes.ok:
            print "error uploading stats to master"
        else:
            print "%s - Upload Status: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), r.content)
            

    @cherrypy.expose
    def index(self):
        return "<html><body><h1>MOSES Grid Manager Node: %s</h1></body></html>" % self.host
    
    # FRONT END FUNCTION CALLS

    @cherrypy.expose
    def region(self, name, action):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"
        #find region in processes, if it exists

        #perform the action
        if action == "add":
            #check if region already present here
            candidate = None
            for proc in self.procs:
                if self.procs[proc].isRegistered:
                    if self.procs[proc].name == name:
                        #duplicate registration, ignore
                        return json.dumps({ "Success": False, "Message": "Region already exists on this Node"})
                else:
                    candidate = self.procs[proc]
            if not candidate:
                return json.dumps({ "Success": false, "Message": "No slots remaining"})
            candidate.registerRegion(name)
            return json.dumps({ "Success": True})
        elif action == "remove":
            #find region, and remove if found
            for proc in self.procs:
                if self.procs[proc].isRegistered and self.procs[proc].name == name:
                    self.procs[proc].deregisterRegion()
                    return json.dumps({ "Success": True})
            return json.dumps({ "Success": False, "Message": "Region not present"})
        elif action == "start":
            for proc in self.procs:
                if self.procs[proc].isRegistered and self.procs[proc].name == name:
                    self.procs[proc].start()
                    return json.dumps({ "Success": True})
            return json.dumps({ "Success": False, "Message": "Region not present"})
        elif action == "stop":
            for proc in self.procs:
                if self.procs[proc].isRegistered and self.procs[proc].name == name:
                    self.procs[proc].stop()
                    return json.dumps({ "Success": True})
            return json.dumps({ "Success": False, "Message": "Region not present"})
        else:
            return json.dumps({ "Success": False, "Message": "Unsupported Action"})
    
    @cherrypy.expose
    def iar(self, name, uname, password, avatarName, avatarPassword, inventoryPath, job, action):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"
        for proc in self.procs:
                if self.procs[proc].isRegistered:
                    if self.procs[proc].name == name:
                        if not self.procs[proc].isRunning:
                            return json.dumps({ "Success": False, "Message": "Region must be running to manage iars"})
                        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
                        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
                        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)
                        if action == "save":
                            if self.procs[proc].saveIar(uname, password, report, upload, inventoryPath, avatarName, avatarPassword):
                                return json.dumps({ "Success": True})
                            return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
                        if action == "load":
                            if self.procs[proc].loadIar(uname, password, ready, report, inventoryPath, avatarName, avatarPassword):
                                return json.dumps({ "Success": True})
                            return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
                        return json.dumps({ "Success": False, "Message": "Invalid action"})
        return json.dumps({ "Success": False, "Message": "Region does not exist on this Host"})
        
    @cherrypy.expose
    def oar(self, name, uname, password, job, action):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"
        for proc in self.procs:
                if self.procs[proc].isRegistered:
                    if self.procs[proc].name == name:
                        if not self.procs[proc].isRunning:
                            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})
                        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
                        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
                        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)
                        if action == "save":
                            if self.procs[proc].saveOar(uname, password, report, upload):
                                return json.dumps({ "Success": True})
                            return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
                        if action == "load":
                            if self.procs[proc].loadOar(uname, password, ready, report):
                                return json.dumps({ "Success": True})
                            return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
                        return json.dumps({ "Success": False, "Message": "Invalid action"})
        return json.dumps({ "Success": False, "Message": "Region does not exist on this Host"})
