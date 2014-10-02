
import psutil, time, json

class Monitor:
    
    def __init__(self):
        self.last_time = time.time()
        psutil.cpu_percent(interval=0)
        self.pnic_vals_before = psutil.network_io_counters(pernic=False)
        self.stats = {}
        
    def updateStatistics(self):
        stats = {}
        hostMem = psutil.phymem_usage()
        stats["memPercent"] = hostMem.percent
        stats["memKB"] = hostMem.used / 1024
        
        stats["cpuPercent"] = psutil.cpu_percent(interval=0, percpu=1)
        
        elapsed = time.time() - self.last_time
        self.last_time = time.time()
        stats["timestamp"] = self.last_time
        pnic_vals_after = psutil.network_io_counters(pernic=False)
        
        stats["netSentPer"] = (pnic_vals_after.bytes_sent - self.pnic_vals_before.bytes_sent)/elapsed
        stats["netRecvPer"] = (pnic_vals_after.bytes_recv - self.pnic_vals_before.bytes_recv)/elapsed
        
        self.pnic_vals_before = pnic_vals_after
        self.stats = stats
        
    def getJsonStats(self):
        stats = self.stats
        return json.dumps(stats)
        
    def bytes2human(self,n):
        """
        >>> bytes2human(10000)
        '9.8 K'
        >>> bytes2human(100001221)
        '95.4 M'
        """
        symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
        prefix = {}
        for i, s in enumerate(symbols):
            prefix[s] = 1 << (i+1)*10
        for s in reversed(symbols):
            if n >= prefix[s]:
                value = float(n) / prefix[s]
                return '%.2f %s' % (value, s)
        return '%.2f B' % (n)
