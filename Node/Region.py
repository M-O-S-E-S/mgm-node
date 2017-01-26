'''
Created on Feb 1, 2013

@author: mheilman
'''
import os, psutil, json, time, requests, threading, re, shutil, sys
from Queue import Queue, Empty
from threading import Thread
from psutil import Popen

from RemoteAdmin import RemoteAdmin

# the pit in which to throw normal process output
DEVNULL = open(os.devnull, 'wb')

class Region:
    """A wrapper class around a psutil popen instance of a region; handling logging and beckground tasks associated with this region"""

    proc = None
    stats = {}
    isRunning = False
    shuttingDown = False

    def __init__(self, regionPort, uuid, name, binDir, regionDir, dispatchUrl, externalAddress, username, password):
        self.port = regionPort
        self.id = uuid
        self.name = name
        self.externalAddress = externalAddress
        self.dispatchUrl = dispatchUrl
        self.startString = "Halcyon.exe -name %s -console rest" % name
        self.username = username
        self.password = password

        self.startDir = os.path.join(regionDir, self.id)
        self.pidFile = os.path.join(self.startDir, 'Halcyon.pid')
        if os.name != 'nt':
            self.startString = "mono %s" % self.startString
        else:
            self.startString = os.path.join(self.startDir, self.startString)

        self.logFile = os.path.join(self.startDir, 'Halcyon.log')

        #start job processing thread
        self.jobQueue = Queue()
        th = threading.Thread(target=self._doTasks)
        th.daemon = True
        th.start()

        #start process monitoring thread
        th = threading.Thread(target=self._monitorProcess)
        th.daemon = True
        th.start()

        #start log monitoring thread
        th = threading.Thread(target=self._monitorLog)
        th.daemon = True
        th.start()

        # attempt process recovery from pidfile
        if os.path.exists(self.pidFile):
            try:
                pid = int(open(self.pidFile).read())
                self.proc = psutil.Process(pid)
                if not "Halcyon.exe" in self.proc.name():
                    self.proc = None
                self.isRunning = self.proc.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING, psutil.STATUS_DISK_SLEEP]
            except:
                self.isRunning = False

        if not self.isRunning:
            # dont potentially delete and replace binaries on running processes!
            self.jobQueue.put(("_checkBinaries", (binDir,)))

    def __del__(self):
        self.shuttingDown = True

    def _monitorProcess(self):
        """Monitor process and update statistics"""
        while not self.shuttingDown:
            if not self.proc:
                self.isRunning = False
            else:
                try:
                    self.isRunning =  self.proc.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING, psutil.STATUS_DISK_SLEEP]
                except psutil.NoSuchProcess:
                    self.isRunning = False
            stats = {}
            stats["timestamp"] = time.time()
            if self.isRunning:
                try:
                    stats["uptime"] = time.time() - self.proc.create_time()
                    stats["memPercent"] = self.proc.memory_percent(memtype="uss")
                    stats["memKB"] = self.proc.memory_full_info().uss / 1024
                    stats["cpuPercent"] = self.proc.cpu_percent(interval=None)
                except:
                    stats = {}
            self.stats =  stats
            time.sleep(5)

    def _monitorLog(self):
        """Monitor log file and append new lines to the internal queue"""
        while not self.shuttingDown and not os.path.isfile(self.logFile):
            time.sleep(5)
        f = open(self.logFile, 'r')
        f.seek(0,2)
        lines = []
        url = "http://%s/dispatch/logs/%s" % (self.dispatchUrl,self.id)
        while not self.shuttingDown:
            line = f.readline()
            if line:
                lines.append(line)
                continue
            else:
                if len(lines) == 0:
                    time.sleep(5)
                    continue

                try:
                    req = requests.post(url,data={'log': json.dumps(lines)}, verify=False)
                    if not req.status_code == requests.codes.ok:
                        print "Error sending %s: %s" % (self.region, req.content)
                    else:
                        #logs uploaded successfully
                        lines = []
                except requests.ConnectionError:
                    print "error uploading logs to master"
                    time.sleep(5)
                    continue

        f.close()

    def _doTasks(self):
        """Process asynchronous lambda tasks from internal queue"""
        while not self.shuttingDown:
            #block and wait for a new job
            try:
                (functor, args) = self.jobQueue.get(False)
            except Empty:
                time.sleep(5)
                continue
            except IOError:
                continue
            try:
                getattr(self, functor)(*args)
            except:
                print "Error processing job %s: %s" % (functor, sys.exc_info())

    def _checkBinaries(self, binDir):
        """make sure we are ready to start the process when necessary"""
        #clean up/create region folder
        if not os.path.isdir(self.startDir):
            shutil.copytree(binDir, self.startDir)
        else:
			#update binaries if they have changed
			if os.path.getmtime(binDir) > os.path.getmtime(self.startDir):
				shutil.rmtree(self.startDir)
				shutil.copytree(binDir, self.startDir)

    def start(self):
        """schedule the process to start"""
        self.jobQueue.put(("_start", ()))

    def _start(self):
        """start the process"""
        if self.isRunning:
            return

        # write the Halcyon.ini config file
        r = requests.get("http://%s/dispatch/process/%s?httpPort=%s&externalAddress=%s" % (self.dispatchUrl, self.id, self.port, self.externalAddress))
        if r.status_code != requests.codes.ok:
            print "Region %s failed to start, failed getting ini values from MGM"
            return
        content = json.loads(r.content)
        region = content["Region"]
        f = open(os.path.join(self.startDir, 'Halcyon.ini'), 'w')
        for section in region:
            f.write('[%s]\n' % section)
            for item in region[section]:
                f.write('\t%s = "%s"\n' %(item, region[section][item]))
        f.close()

        # write the Regions.cfg file
        r = requests.get("http://%s/dispatch/region/%s" % (self.dispatchUrl, self.id))
        if r.status_code != requests.codes.ok:
            print "Region %s failed to start, failed getting region values from MGM"
            return
        region = json.loads(r.content)["Region"]
        xml = """<Root><Config allow_alternate_ports="false" clamp_prim_size="false"
            external_host_name="{5}" internal_ip_address="0.0.0.0" internal_ip_port="{4}"
            lastmap_refresh="0" lastmap_uuid="00000000-0000-0000-0000-000000000000"
            master_avatar_first="first" master_avatar_last="last"
            master_avatar_pass="23459873204987wkjhbao873q4tr7u3q4of7"
            master_avatar_uuid="00000000-0000-0000-0000-000000000000"
            nonphysical_prim_max="0" object_capacity="0" outside_ip="{5}"
            physical_prim_max="0" region_access="0" region_product="0"
            sim_UUID="{0}" sim_location_x="{2}" sim_location_y="{3}"
            sim_name="{1}" /></Root>""".format(
                self.id,
                region["Name"],
                region["LocationX"],
                region["LocationY"],
                self.port,
                self.externalAddress
                )
        if not os.path.exists(os.path.join(self.startDir, 'Regions')):
            os.mkdir(os.path.join(self.startDir, 'Regions'))
        f = open(os.path.join(self.startDir, 'Regions', 'default.xml'), 'w')
        f.write(xml)
        f.close()

        self.proc = Popen(self.startString.split(" "), cwd=self.startDir, stdout=DEVNULL, stderr=DEVNULL)
        #write a pidfile
        f = open(self.pidFile, 'w')
        f.write(str(self.proc.pid))
        f.close()

    def stop(self):
        """schedule the process to exit"""
        self.jobQueue.put(("_stop", ()))

    def _stop(self):
        #if os.path.exists(self.pidFile):
        #    os.remove(self.pidFile)
        radmin = RemoteAdmin("127.0.0.1", self.port, self.username, self.password)
        if not radmin.connected:
            print "Cannot shutdown region %s: %s" % (self.uuid, radmin.message)
            return
        radmin.shutdown(self.id, 0)
        radmin.close()

    def kill(self):
        """immediately terminate the process"""
        if os.path.exists(self.pidFile):
            os.remove(self.pidFile)
        try:
            self.proc.kill()
        except psutil.NoSuchProcess:
            pass

    def saveOar(self, reportUrl, uploadUrl):
        """schedule an oar save and upload to MGM"""
        try:
            self.jobQueue.put(("_saveOar", (reportUrl, uploadUrl,)))
        except:
            return False
        return True

    def _saveOar(self, reportUrl, uploadUrl):
        """perform the actual oar load"""
        if not self.isRunning:
            print "Save oar aborted, region is not running"
            requests.post(reportUrl, data={"Status": "Error: Region is not running"}, verify=False)
            return
        oarFile = os.path.join(self.startDir, '%s.oar' % self.name)
        statusFile = os.path.join(self.startDir, '%s.oarstatus' % self.name)
        if os.path.exists(oarFile):
            print "Removing existing oarfile %s" % oarFile
            os.remove(oarFile)
        if os.path.exists(statusFile):
            print "Removing existing statusFile %s" % statusFile
            os.remove(statusFile)
        radmin = RemoteAdmin("127.0.0.1", self.port, self.username, self.password)
        if not radmin.connected:
            print "Save oar aborted, could not connect to remote admin"
            requests.post(reportUrl, data={"Status": "Error: Could not connect to remoteAdmin"}, verify=False)
            return
        print "backing up to %s" % oarFile
        success, msg = radmin.backup(self.name, oarFile, True)
        radmin.close()
        if not success:
            print "Save oar aborted, error: %s" % msg
            requests.post(reportUrl, data={"Status": "Error: %s" % msg}, verify=False)
            return

        print "Save oar triggered in region %s" % self.id
        requests.post(reportUrl, data={"Status": "Saving..."}, verify=False)

        #wait for statusfile to be written on completion
        while self.isRunning and not os.path.exists(statusFile):
            time.sleep(5)

        def is_open(file_name):
            if os.path.exists(file_name):
                try:
                    os.rename(file_name,file_name)
                    return False
                except:
                    return True
            raise NameError

        #wait for process to close the archive
        while self.isRunning and is_open(oarFile):
            time.sleep(5)

        if not self.isRunning:
            requests.post(reportUrl, data={"Status": "Error: region halted during save"}, verify=False)
            return

        # check statusfile
        print "Save oar complete for region %s" % self.id
        with open(statusFile, 'rb') as f:
            data = f.read()
            if data[0] == '\x01':
                # success
                print "Save oar for region %s succeeded" % self.id
                r = requests.post(uploadUrl, data={"Success": True}, files={'file': (self.name, open(oarFile, 'rb'))}, verify=False)
            else:
                #failure
                print "Save oar for region %s failed for unspecified reason" % self.id
                requests.post(reportUrl, data={"Status": "Error: an unknown error occurred while saving the oar file"}, verify=False)

    def loadOar(self, readyUrl, reportUrl):
        """schedule an oar download from MGM and load into region"""
        try:
            self.jobQueue.put(("_loadOar", (readyUrl, reportUrl,)))
        except:
            return False
        return True

    def _loadOar(self, readyUrl, reportUrl):
        """ download an oar from MGM and load it into the region"""
        print "loading an oar file from %s" % readyUrl
        requests.post(reportUrl, data={"Status": "Loading onto host"}, verify=False)
        oarFile = os.path.join(self.startDir, '%s.oar' % self.name)

        response = requests.get(readyUrl, stream=True)
        if not response.ok:
            requests.post(reportUrl, data={"Status": "Error: Could not download file from MGM"}, verify=False)
            print "Error loading OAR file from MGM"
            return

        with open(oarFile, 'wb') as handle:
            for block in response.iter_content(chunk_size=1024):
                handle.write(block)

        requests.post(reportUrl, data={"Status": "Loading into the Region"}, verify=False)
        radmin = RemoteAdmin("127.0.0.1", self.port, self.username, self.password)
        if not radmin.connected:
            print "Load oar aborted, could not connect to remote admin"
            requests.post(reportUrl, data={"Status": "Error: Could not connect to remoteAdmin"}, verify=False)
            return
        print "Triggering restore of file %s" % oarFile
        success, msg = radmin.restore(self.name, oarFile, True, True)
        radmin.close()
        if not success:
            print "Load oar aborted, error: %s" % msg
            requests.post(reportUrl, data={"Status": "Error: %s" % msg}, verify=False)
            return

        requests.post(reportUrl, data={"Status": "Done"}, verify=False)

    def consoleCmd(self, cmd):
        """synchronous console command"""
        print "Executing command: %s" % cmd
        radmin = RemoteAdmin("127.0.0.1", self.port, self.username, self.password)
        if not radmin.connected:
            return False
        success, msg = radmin.command(self.name, cmd)
        radmin.close()
        if not success:
            return False
        return True
