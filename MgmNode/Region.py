'''
Created on Feb 1, 2013

@author: mheilman
'''
import os, psutil, json, time, requests, threading, Queue as QX, re, shutil
from multiprocessing import Process, Queue
from subprocess import PIPE


from psutil import Popen

from RestConsole import RestConsole

class RegionLogger( Process ):

    def __init__(self, url, region, stderr, stdout, queue):
        super(RegionLogger, self).__init__()
        self.queue = queue
        self.regexp = re.compile(r'^{[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}}.*')
        self.url = "http://%s/dispatch/logs/%s" % (url,region)
        self.region = region
        self.stderr = stderr
        self.stdout = stdout
        
    def run(self):
        self.stdin = threading.Thread(target=self.logToOut, args=(self.stdout,))
        self.stdin.daemon = True
        self.stdin.start()
        self.stdout = threading.Thread(target=self.logToOut, args=(self.stderr,))
        self.stdout.daemon = True
        self.stdout.start()
        
        try:
            while True:
                if not self.queue.empty():
                    messages = []
                    try:
                        for i in range(self.queue.qsize()):
                            line = self.queue.get(False)
                            if self.regexp.search(line) is not None:
                                parts = line.split("-", 1)
                                log = {}
                                log["timestamp"] = "%s %s" % (time.strftime("%Y-%m-%d"), parts[0].strip())
                                log["message"] = parts[1].strip()
                                messages.append(log)
                            else:
                                log = {}
                                log["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
                                log["message"] = line
                                messages.append(log)
                    except QX.Empty:
                        pass

                    if len(messages) > 0:
                        req = requests.post(self.url,data={'log':json.dumps(messages)}, verify=False)
                        if not req.status_code == requests.codes.ok:
                            print "Error sending %s: %s" % (self.region, req.content)
                time.sleep(10)
        except:
            #we receive sigint and sigterm during normal operation, exit
            pass
	
    def logToOut(self, pipe):
        while True:
            line = pipe.readline()
            if line.strip() != "":
                self.queue.put(line.strip())
            #time.sleep(.05)

class RegionWorker( Process ):
    def __init__(self, jobQueue, logQueue):
        super(RegionWorker, self).__init__()
        self.queue = jobQueue
        self.log = logQueue
    
    def run(self):
        self.log.put("[MGM] region worker process started")
        try:
            while True:
                #block and wait for a new job
                job = self.queue.get()
                
                #operate
                if job['name'] == "save_oar":
                    self.saveOar(job["reportUrl"],job["uploadUrl"], job["console"],job["location"],job["region"])
                elif job['name'] == "load_oar":
                    self.loadOar(job["ready"],job["report"], job["console"])
                elif job['name'] == "save_iar":
                    self.saveIar(job["report"],job["upload"],job["console"], job["path"], job["user"], job["password"], job["location"])
                elif job['name'] == "load_iar":
                    self.loadIar(job["ready"],job["report"], job["console"], job["path"], job["user"], job["password"])
                else:
                    print "invalid job %s, ignoring" % job['name']
        except:
            self.log.put("[MGM] region worker process exiting")
            #we most likely received a sigint
            pass
                
    def loadIar(self, ready, report, console, inventoryPath, user, password):
        self.log.put("[MGM] starting load iar task")
        console.read()
        start = time.time()
        cmd = "load iar %s %s %s %s" % (user, inventoryPath, password, ready)
        console.write(cmd);
        done = False
        abort = False
        while not done and not abort:
            for line in console.readLine():
                #timeout this function after an hour
                if (time.time() - start) > 60*60:
                    abort = True
                    continue
                if not "[INVENTORY ARCHIVER]" in line:
                    continue
                if not "Loaded archive" in line:
                    continue
                done = True
                break
                
        console.close()
        if done:
            # report back to master
            r = requests.post(report, data={"Success": True, "Done": True}, verify=False)
            self.log.put("[MGM] load iar completed successfully")
            return
        if abort:
            r = requests.post(report, data={"Success": False, "Done": True, "Message": "Timeout.  Iar load took too long"}, verify=False)
            self.log.put("[MGM] load iar did not complete within time limit")
            return
        r = requests.post(report, data={"Success": False, "Done": True, "Message": "Unknown error"}, verify=False)
        self.log.put("[MGM] load iar unknown error")
        print "An error occurred loading iar file, we are not aborted or done"
        
    def saveIar(self, report, upload, console, inventoryPath, user, password, iarDir):
        console.read()
        start = time.time()
        iarName = "%s.iar" % (user.replace(" ",""))
        cmd = "save iar %s %s %s %s" % (user, inventoryPath, password, iarName)
        console.write(cmd)
        done = False
        abort = False
        while not done and not abort:
            for line in console.readLine():
                #timeout this function after an hour
                if (time.time() - start) > 60*60:
                    abort = True
                    continue
                if not "[INVENTORY ARCHIVER]" in line:
                    continue
                if not "Saved archive" in line:
                    continue
                done = True
                break
        
        console.close()
        if done:
            #post iar file back to mgm with job number
            iar = os.path.join(iarDir,iarName)
            r = requests.post(upload, data={"Success": True}, files={'file': (iarName, open(iar, 'rb'))}, verify=False)
            os.remove(iar)
            self.log.put("[MGM] save iar completed successfully")
            return
        if abort:
            r = requests.post(report, data={"Success": False, "Done": True, "Message": "Timeout.  Iar save took too long"}, verify=False)
            self.log.put("[MGM] save iar did not complete within the time limit")
            return
        r = requests.post(report, data={"Success": False, "Done": True, "Message": "Unknown error"}, verify=False)
        self.log.put("[MGM] save iar unknown error")
        print "An error occurred saving iar file, we are not aborted or done"
    
    def loadOar(self, ready, report, console):
        self.log.put("[MGM] starting load oar task")
        console.read()
        start = time.time()
        cmd = "load oar %s" % ready
        console.write(cmd);
        done = False
        abort = False
        while not done and not abort:
            for line in console.readLine():
                #timeout this function after an hour
                if (time.time() - start) > 60*60:
                    abort = True
                    continue
                if not "[ARCHIVER]" in line:
                    continue
                if not "Successfully" in line:
                    continue
                done = True
                break
                
        console.close()
        if done:
            # report back to master
            r = requests.post(report, data={"Success": True, "Done": True}, verify=False)
            self.log.put("[MGM] load oar completed successfully")
            return
        if abort:
            r = requests.post(report, data={"Success": False, "Done": True, "Message": "Timeout.  Oar load took too long"}, verify=False)
            self.log.put("[MGM] load oar did not complete within the time limit")
            return
        r = requests.post(report, data={"Success": False, "Done": True, "Message": "Unknown error"}, verify=False)
        self.log.put("[MGM] load oar unknown error")
        print "An error occurred loading oar file, we are not aborted or done"
        
    def saveOar(self, report, upload, console, oarDir, regionName):
        self.log.put("[MGM] starting save oar task")
        console.read()
        start = time.time()
        oarName = "%s.oar" % regionName
        cmd = "save oar %s" % oarName
        console.write(cmd)
        done = False
        abort = False
        while not done and not abort:
            for line in console.readLine():
                #timeout this function after an hour
                if (time.time() - start) > 60*60:
                    abort = True
                    continue
                if not "[ARCHIVER]" in line:
                    continue
                if not "Finished" in line:
                    continue
                done = True
                break
        
        console.close()
        if done:
            #post oar file back to mgm with job number
            oar = os.path.join(oarDir,oarName)
            r = requests.post(upload, data={"Success": True}, files={'file': (regionName, open(oar, 'rb'))}, verify=False)
            self.log.put("[MGM] save oar completed successfully")
            os.remove(oar)
            return
        if abort:
            r = requests.post(report, data={"Success": False, "Done": True, "Message": "Timeout.  Oar save took too long"}, verify=False)
            self.log.put("[MGM] save oar did not complete within the time limit")
            return
        r = requests.post(report, data={"Success": False, "Message": "Unknown error"}, verify=False)
        self.log.put("[MGM] save oar unknown error")
        print "An error occurred saving oar file, we are not aborted or done"

class Region:
    def __init__(self, regionPort, consolePort, procName, binDir, regionDir, dispatchUrl, externalAddress):
        self.isRegistered = False    
        self.proc = None
        self.stats = {}
        
        self.port = regionPort
        self.console = consolePort
        self.name = procName
        self.externalAddress = externalAddress
        self.dispatchUrl = dispatchUrl
        dispatcher = "http://%s/dispatch/process/%%s?httpPort=%%s&consolePort=%%s&externalAddress=%%s" % dispatchUrl
        self.startString = "OpenSim.exe -console rest -inifile %s -logconfig %s.cfg" % (dispatcher, procName)

        self.trackStage = "stopped"
        self.startFailCounter = 0
        self.stopFailCounter = 0
        self.simStatsCounter = 0
        self.simPhysicsCounter = 0
        
        if os.name != 'nt':
            self.startString = "mono %s" % self.startString
        else:
            self.startString = self.startDir + self.startString
            
        #clean up/create region folder
        self.startDir = os.path.join(regionDir, self.name)
        if not os.path.isdir(self.startDir):
            shutil.copytree(binDir, self.startDir)
                
        self.configFile = os.path.join(self.startDir, '%s.cfg' % procName)
        self.exe = os.path.join(self.startDir, 'OpenSim.exe')
        
        #set up threaded worker
        self.jobQueue = Queue()
        self.logQueue = Queue()
        self.workerProcess = None
        self.loggerProcess = None
        
    def __del__(self):
        self.terminate()
            
    def terminate(self):
        if self.proc:
            try:
                self.proc.kill()
                #self.proc.communicate()
                #delf.proc = None
            except:
                pass
        if self.workerProcess:
            self.workerProcess.terminate()
        if self.loggerProcess:
            self.loggerProcess.terminate()
            
    def isRunning(self):
        if not self.proc:
            return False
        try:
            if self.proc.status in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING, psutil.STATUS_DISK_SLEEP]:
                return True
        except psutil.NoSuchProcess:
            return False
        return False
    
    def getJsonStats(self):
        stats = self.stats
        return json.dumps(stats)
    
    def update(self):
        #update stats
        self.updateProcStats()
      
    def updateProcStats(self):
        """we can access most of this for the web, but we are compiling it to send xml to MGM"""
        stats = {}
        stats["stage"] = self.trackStage	
        if self.isRunning():
			try:
				stats["uptime"] = time.time() - self.proc.create_time
				stats["memPercent"] = self.proc.get_memory_percent()
				stats["memKB"] = self.proc.get_memory_info().rss / 1024
				stats["cpuPercent"] = self.proc.get_cpu_percent(0.1)
				r = requests.get("http://127.0.0.1:%d/jsonSimStats" % self.port, timeout=0.25)
				if r.status_code == requests.codes.ok:
					stats["simStats"] = json.loads(r.content)
					if float(self.stats["simStats"]["SimFPS"]) == 0.0:
						#error condition
						self.simPhysicsCounter+= 1
						print "process %s reported 0 physics fps" % self.name
					else:
						self.simPhysicsCounter = 0
				else:
					self.simStatsCounter+=1
					print "process %s did not report simstats" % self.name
			except:
				# ignore and track.  we get nosuchprocess regularly here when first starting a region
				self.simStatsCounter+= 1
        self.stats =  stats
        
        if not self.isRunning() and self.trackStage == "running":
            self.startFailCounter += 1
            #self.startProcess()
            print "process %s is not running" % self.name
        if self.isRunning() and self.trackStage == "stopped":
            self.stopFailCounter += 1
            #self.stopProcess()
            print "process %s is not stopped" % self.name
            
        if self.startFailCounter > 5 and not self.trackStage == "Failed to Start":
            self.trackStage = "Failed to Start"
            self.logQueue.put("[MGM] %s failed to start 5 times, autostart halted" % self.name)
            print "process %s autostart halted" % self.name
        
        if self.stopFailCounter > 5 and not self.trackStage == "Failed to Stop":
            self.trackStage = "killed"
            self.stopFailCounter = 0
            self.stopProcess()
            self.logQueue.put("[MGM] %s failed to stop within 5 cycles, it has been terminated" % self.name)
            print "process %s autostop halted" % self.name
        
        if self.simStatsCounter > 5 and not self.trackStage == "Killed":
            self.stopProcess()
            self.trackStage = "Killed"
            self.simStatsCounter = 0
            self.logQueue.put("[MGM] %s failed to report simstats for 5 cycles, it has been terminated" % self.name)
            print "process %s killed for not reporting simstats" % self.name
          
        if self.simPhysicsCounter > 5 and not self.trackstage == "Killed":
            self.stopProcess()
            self.trackStage = "Killed"
            self.simPhysicsCounter = 0
            self.logQueue.put("[MGM]%s had 0 FPS physics for 5 cycles, it has been terminated" % self.name)
            print "process %s killed for poor performance" % self.name
           
    def writeConfig(self, name):
        cfgFile = open(self.configFile, "w")
        cfgFile.write( """<?xml version="1.0" encoding="utf-8" ?>
<configuration>
  <configSections>
    <section name="log4net" type="log4net.Config.Log4NetConfigurationSectionHandler,log4net" />
  </configSections>
  <runtime>
    <gcConcurrent enabled="true" />
        <gcServer enabled="true" />
  </runtime>
  <appSettings></appSettings>
  <log4net>
    <appender name="Console" type="OpenSim.Framework.Console.OpenSimAppender, OpenSim.Framework.Console">
      <layout type="log4net.Layout.PatternLayout">
        <conversionPattern value="%date{{HH:mm:ss}} - %message" />
      </layout>
    </appender>
    <appender name="StatsLogFileAppender" type="log4net.Appender.FileAppender">
      <file value="OpenSimStats.log"/>
      <appendToFile value="true" />
      <layout type="log4net.Layout.PatternLayout">
        <conversionPattern value="%date - %message%newline" />
      </layout>
    </appender>
    <root>
      <level value="DEBUG" />
      <appender-ref ref="Console" />
    </root>
    <logger name="OpenSim.Region.ScriptEngine.XEngine">
      <level value="INFO"/>
    </logger>
	<logger name="special.StatsLogger">
      <appender-ref ref="StatsLogFileAppender"/>
    </logger>
  </log4net>
</configuration>
""")
        cfgFile.close()
    
    def deregisterRegion(self):
        self.isRegistered = False
        self.stopProcess()
    
    def registerRegion(self, name):
        self.name = name
        self.isRegistered = True
    
    def start(self):
        self.logQueue.put("[MGM] %s user requested to start" % self.name)
        self.trackStage = "running"
        self.startFailCounter = 0
        self.simStatsCounter = 0
        self.simPhysicsCounter = 0
        self.stopFailCounter = 0
        threading.Thread(target=self.startProcess)
        self.startProcess()
    
    def startProcess(self):
        if self.isRunning() or not self.isRegistered:
            return
        
        if self.workerProcess:
            self.workerProcess.terminate()
        if self.loggerProcess:
			self.loggerProcess.terminate()
        
        self.writeConfig(self.name)
        
        self.logQueue.put("[MGM] %s starting" % self.name)
        
        print "Region %s Started" % self.name
        namedString = self.startString % (self.name, self.port, self.console, self.externalAddress)
        mono_env = os.environ.copy()
        mono_env["MONO_THREADS_PER_CPU"] = "2000"
        self.proc = Popen(namedString.split(" "), cwd=self.startDir, stdout=PIPE, stderr=PIPE, env=mono_env)
        self.workerProcess = RegionWorker(self.jobQueue, self.logQueue)
        self.workerProcess.daemon = True
        self.workerProcess.start()
        self.loggerProcess = RegionLogger(self.dispatchUrl, self.name, self.proc.stdout, self.proc.stderr, self.logQueue)
        self.loggerProcess.daemon = True
        self.loggerProcess.start()
    
    def stop(self):
        self.trackStage = "stopped"
        self.stopFailCounter = 0
        self.startFailCounter = 0
        self.simStatsCounter = 0
        self.simPhysicsCounter = 0
        self.logQueue.put("[MGM] %s requested Stop" % self.name)
        
    def stopProcess(self):
        if self.isRunning():
            self.logQueue.put("[MGM] %s Terminating" % self.name)
            self.workerProcess.terminate()
            self.workerProcess = None
            try:
                self.proc.kill()
                #self.proc.communicate()
            except psutil.NoSuchProcess:
                pass
            self.loggerProcess.terminate()
            self.loggerProcess = None

    def saveOar(self, uname, password, reportUrl, uploadUrl):
        try:
            self.logQueue.put("[MGM] %s requested save oar" % self.name)
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, uname, password)
            self.jobQueue.put({"name": "save_oar", "reportUrl": reportUrl, "uploadUrl": uploadUrl, "console": console, "region":self.name, "location":self.startDir})
        except:
            return False
        return True
        
    def loadOar(self, uname, password, ready, report ):
        try:
            self.logQueue.put("[MGM] %s requested load oar" % self.name)
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, uname, password)
            self.jobQueue.put({"name": "load_oar", "ready": ready, "report": report, "console": console})
        except:
            print "exception occurred"
            return False
        return True
        
    def loadIar(self, uname, pword, ready, reportUrl, invPath, avatar, password):
        try:
            self.logQueue.put("[MGM] User requested load iar")
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, uname, pword)
            self.jobQueue.put({"name": "load_iar", "ready": ready, "report": reportUrl, "console": console, "path": invPath, "user":avatar, "password":password})
        except:
            return False
        return True
        
    def saveIar(self, uname, pword, reportUrl, uploadUrl, invPath, avatar, password):
        try:
            self.logQueue.put("[MGM] User requested save iar")
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, uname, pword)
            self.jobQueue.put({"name": "save_iar", "report": reportUrl, "upload": uploadUrl, "console": console, "path": invPath, "user":avatar, "password":password, "location":self.startDir})
        except:
            return False
        return True
