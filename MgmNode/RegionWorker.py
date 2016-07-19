from psutil import Popen

class RegionWorker( Thread ):
    """a threaded class to handle long running tasks that may output files locally"""

    def __init__(self, jobQueue, logQueue):
        super(RegionWorker, self).__init__()
        self.queue = jobQueue
        self.shutdown = False
        self.log = logQueue

    consumeLog(self, line):
        pass

    def run(self):
        self.log.put("[MGM] region worker process started")
        try:
            while not self.shutdown:
                time.sleep(5)
                #block and wait for a new job
                try:
                    job = self.queue.get(False)
                except QX.Empty:
                    continue

                #operate
                if job['name'] == "save_oar":
                    self.saveOar(job["reportUrl"],job["uploadUrl"], job["console"],job["location"],job["region"])
                elif job['name'] == "load_oar":
                    self.loadOar(job["ready"],job["report"], job["console"], job["merge"], job["x"], job["y"], job["z"])
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
            time.sleep(5)
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
            time.sleep(5)
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
            r = requests.post(upload, data={"Success": True}, files={'file': (user, open(iar, 'rb'))}, verify=False)
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

    def loadOar(self, ready, report, console, merge, x, y, z):
        self.log.put("[MGM] starting load oar task")
        console.read()
        start = time.time()
        if merge == "1":
            cmd = "load oar --merge --force-terrain --force-parcels --displacement <%s,%s,%s> %s" % (x, y, z, ready)
        else:
            cmd = "load oar --displacement <%s,%s,%s> %s" % (x, y, z, ready)
        console.write(cmd);
        done = False
        abort = False
        while not done and not abort:
            time.sleep(5)
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
            time.sleep(5)
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
