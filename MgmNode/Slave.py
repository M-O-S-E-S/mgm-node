'''
Created on Jan 30, 2013

@author: mheilman
'''

import os

from Region import Region
import time, uuid, requests, logging, logging.handlers, json
from os import listdir
from os.path import isfile, join
from Monitor import Monitor as MonitorWrapper

import xml.etree.ElementTree as ET

from twisted.web import server, resource
from twisted.internet import reactor, task

class Slave(resource.Resource):
    isLeaf = True
    def getChild(self, name, request):
        if name == '':
            return self
        return Resource.getChild(self, name, request)

    def render_GET(self, request):
        return "<html><body><h1>MOSES Grid Manager Node: %s</h1></body></html>" % self.host

    #receive commands from the frontend
    #TODO, switch arguments to url parameters instead of posted action argument
    def render_POST(self, request):
        #perform ip verification
        if not request.getClientIP() == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % request.getClientIP()
            return "Denied, this functionality if restricted to the mgm web app"
        urlParts = request.uri.split("/")
        if len(urlParts) < 2:
            return json.dumps({ "Success": False, "Message": "Invalid Route"})
        if urlParts[1] == "region":
            if not 'action' in request.args or not 'name' in request.args:
                return json.dumps({ "Success": False, "Message": "Invalid arguments"})
            return self.region(request.args['action'][0],request.args['name'][0])
        elif urlParts[1] == "loadIar":
            if not 'avatarName' in request.args or not 'job' in request.args or not 'inventoryPath' in request.args or not 'avatarPassword' in request.args or not 'name' in request.args:
                return json.dumps({ "Success": False, "Message": "Invalid arguments"})
            return self.loadIar(
                request.args['name'][0],
                request.args['avatarName'][0],
                request.args['avatarPassword'][0],
                request.args['inventoryPath'][0], 
                request.args['job'][0])
        elif urlParts[1] == "saveIar":
            #saveIar
            if not 'avatarName' in request.args or not 'job' in request.args or not 'inventoryPath' in request.args or not 'avatarPassword' in request.args or not 'name' in request.args:
                return json.dumps({ "Success": False, "Message": "Invalid arguments"})
            return self.saveIar(
                request.args['name'][0],
                request.args['avatarName'][0],
                request.args['avatarPassword'][0],
                request.args['inventoryPath'][0], 
                request.args['job'][0])
        elif urlParts[1] == "saveOar":
            if not 'job' in request.args or not 'name' in request.args:
                return json.dumps({ "Success": False, "Message": "Invalid arguments"})
            return self.saveOar(request.args['name'][0],request.args['job'][0])
        elif urlParts[1] == "loadOar":
            if not 'job' in request.args or not 'name' in request.args:
                return json.dumps({ "Success": False, "Message": "Invalid arguments"})
            if not 'merge' in request.args or not 'x' in request.args or not 'y' in request.args or not 'z' in request.args:
                return json.dumps({ "Success": False, "Message": "Invalid arguments"})
            return self.loadOar(
                request.args['name'][0], 
                request.args['job'][0], 
                request.args['merge'][0], 
                request.args['x'][0], 
                request.args['y'][0], 
                request.args['z'][0])
        
        return json.dumps({ "Success": False, "Message": "Error, message not handled"})
    
    def __init__(self, conf):
        self.availablePorts = []
        self.registeredRegions = {}
        
        statsInterval = conf['interval']
        self.nodePort = conf['port']
        self.key = uuid.uuid4()
        self.host = conf['host']
        self.frontendAddress = conf['webAddress']
        self.binDir = conf['binDir']
        self.regionDir = conf['regionDir']
        self.publicAddress = conf['regionAddress']
        for r,c in zip(conf['regionPorts'],conf['consolePorts']):
            self.availablePorts.append({"port":r,"console":c})
        
        #we can't start up without our master config
        configLoaded = False
        while not configLoaded:
            try:
                configLoaded = self.loadRemoteConfig()
            except Exception, e:
                print e
                time.sleep(10)
        
        self.monitor = MonitorWrapper()
        
        recurring = task.LoopingCall(self.updateStats)
        recurring.start(statsInterval)
    
    def loadRemoteConfig(self):
        #load additional config from master service
        url = "http://%s/server/dispatch/node" % (self.frontendAddress)
        r = requests.post(url, data={'host':self.host, 'port':self.nodePort, 'key':self.key, 'slots': len(self.availablePorts)}, verify=False)
        if not r.status_code == requests.codes.ok:
            raise Exception("Error contacting MGM at %s" % url)
        
        result = json.loads(r.content)
        
        if not result["Success"]:
            raise Exception("Error loading config: %s" % result["Message"])
            
        if len(result['Regions']) > len(self.availablePorts):
            raise Exception("Error: too many regions for configured ports")
                
        for region in result['Regions']:
            port = self.availablePorts.pop(0)
            self.registeredRegions[region['name']] = {
                "proc": Region(port["port"],port["console"], region['name'], self.binDir, self.regionDir, self.frontendAddress, self.publicAddress),
                "port": port
            }
        return True
        
    def updateStats(self):
        self.monitor.updateStatistics()
        stats = {}
        stats['host'] = self.monitor.stats
        stats['processes'] = []
        
        for name,region in self.registeredRegions.iteritems():
            region["proc"].updateProcStats()
            p = {}
            p['name'] = name
            p['running'] = str(region["proc"].isRunning())
            p['stats'] = region["proc"].stats
            stats['processes'].append(p)
        
        url = "http://%s/server/dispatch/stats/%s" % (self.frontendAddress, self.host)
        try:
            r = requests.post(url, data={"json": json.dumps(stats)}, verify=False)
        except requests.ConnectionError:
            print "error connecting to master"
            return
        if not r.status_code == requests.codes.ok:
            print "error uploading stats to master"
        else:
            print "%s - Upload Status: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), r.content)

    # FRONT END FUNCTION CALLS

    def region(self, action, name):
        if action == "add":
            #check if region already present here
            if name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region already exists on this Node"})
            try:
                port = self.availablePorts.pop(0)
            except Exception, e:
                return json.dumps({ "Success": False, "Message": "No slots remaining"})
            self.registeredRegions[name] = {
                "proc": Region(port["port"],port["console"], name, self.binDir, self.regionDir, self.frontendAddress, self.publicAddress),
                "port": port
            }
            return json.dumps({ "Success": True})
        elif action == "remove":
            #find region, and remove if found
            if not name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region not present"})
            if self.registeredRegions[name]["proc"].isRunning():
                return json.dumps({ "Success": False, "Message": "Region is still running"})
            port = self.registeredRegions[name]["port"]
            del self.registeredRegions[name]
            self.availablePorts.insert(0,port)
            return json.dumps({ "Success": True})
        elif action == "start":
            if not name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region not present"})
            self.registeredRegions[name]["proc"].start()    
            return json.dumps({ "Success": True})
        elif action == "stop":
            if not name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region not present"})
            self.registeredRegions[name]["proc"].stop()
            return json.dumps({ "Success": True})
        else:
            return json.dumps({ "Success": False, "Message": "Unsupported Action"})
    
    def loadIar(self, name, avatarName, avatarPassword, inventoryPath, job):
        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage iars"})
                         
        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)

        if self.registeredRegions[name]["proc"].loadIar(ready, report, inventoryPath, avatarName, avatarPassword):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
        
    def saveIar(self, name, avatarName, avatarPassword, inventoryPath, job):
        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage iars"})
                         
        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)
        if self.registeredRegions[name]["proc"].saveIar(report, upload, inventoryPath, avatarName, avatarPassword):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
    
    def saveOar(self, name, job):
        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})
            
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)
        if self.registeredRegions[name]["proc"].saveOar(report, upload):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
        
    def loadOar(self, name, job, merge, x, y, z):
        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})
            
        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        if self.registeredRegions[name]["proc"].loadOar(ready, report, merge, x, y, z):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
