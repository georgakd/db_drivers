import sys
import random
import time
import timeit
import measurements
try:
    import redis
except ImportError, e:
    sys.exit('\nPython Redis driver not installed, or not on PYTHONPATH.\n'
             'You might try "pip install redis".\n'
             'Python: %s\n'
             'Module load path: %r\n'
             'Error: %s\n' % (sys.executable, sys.path, e))

class RespOperation(object):
    """Common class for all Resp Operations"""
    def __init__(self, host='localhost', port='6379', subs=10, test_load='no'):
        self.host = host
        self.port = port
        self.subs = subs
        self.test_load = test_load
        self.min_uid = 1
        self.max_uid = 100000
        self.key_list = random.sample(xrange(self.min_uid, self.max_uid + 1), self.subs)
        self.hkey_list = random.sample(xrange(self.min_uid, self.max_uid + 1), self.subs)
        self.sorted_list = random.sample(xrange(self.min_uid, self.max_uid + 1), self.subs)
        try:
            self.resp_conn = redis.StrictRedis(self.host, self.port)
            self.dummy_response = self.resp_conn.get(None)  # test if the server is down with a dummy query
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'

    def executeCommandWrapper(self, func, *args, **kwargs):
        """Wrapper function to transform redis commands with args to commands with no args for using it in timeit.
           It is based on the FunctionWrapper design pattern."""
        def executeCommandWrapped():
            return func(*args, **kwargs)
        return executeCommandWrapped
                
    def validLoadOpts(self):
        load_list = ['set', 'get', 'delete', 'hset', 'hgetall', 'hdel', 'zadd', 'zscore', 'zrange', 'zrem', 'no']
        return self.test_load in load_list
        
    def chooseLoad(self, which_load):             
        if not self.validLoadOpts():
            print 'Unknown load...try again'
            raise SystemExit
            
        start = time.time()
        print('Started measuring execution time.....')
        if which_load == 'set':
            self.operSet()
        elif which_load == 'hset':
            self.operHset()
        elif which_load == 'zadd':
            self.operZadd()         
        elif which_load == 'get':
            self.operSet() 
            self.operGet()
        elif which_load == 'hgetall':
            self.operHset() 
            self.operHgetall()
        elif which_load == 'zscore':
            self.operZadd()
            self.operZscore()
        elif which_load == 'zrange':
            self.operZadd()
            self.operZrange()
        elif which_load == 'delete':
            self.operSet() 
            self.operDelete()
        elif which_load == 'hdel':
            self.operHset()
            self.operHdel()
        elif which_load == 'zrem':
            self.operZadd()
            self.operZrem()    
        elif which_load == 'no':
            print 'No load used'
        end = time.time()
        print('Operations took %r sec' % (end - start))
        measurements.calcThroughput(len(self.key_list), (end - start))  #  or use hkey or sorted lists
    
    def operSet(self):
        try:
            latencyList = []
            for i in self.key_list:
                value = 'StringTestValue'+str(i)
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.set, i, value)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.key_list), latencyList, 'Sets')
            print '%r subscribers set' % len(self.key_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'    
                
    def operGet(self):   
        try:
            latencyList = []
            for i in self.key_list:
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.get, i)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.key_list), latencyList, 'Gets')
            print '%r subscribers get' % len(self.key_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'      
                
    def operDelete(self):
        try:
            latencyList = []
            for i in self.key_list:
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.delete, i)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.key_list), latencyList, 'Deletes')
            print '%r subscribers deleted' % len(self.key_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'                           
    
    def operHset(self):
        try:
            latencyList = []
            for i in self.hkey_list:
                value = 'HashTestValue'+str(i)
                # how to check return status of self.resp_conn.hset when used in the wrapper?
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.hset, 'test_hash', i, value)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.hkey_list), latencyList, 'Hsets')
            print '%r subscribers hset' % len(self.hkey_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'                   
    
    def operHgetall(self):
        try:
            self.resp_conn.hgetall('test_hash')
            executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.hgetall, 'test_hash')
            latency = timeit.timeit(executeCommandWrapped, number=1)
            print 'The hgetall latency is %r' % latency
            print '%r subscribers hgetall' % len(self.hkey_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'      
            
    def operHdel(self):
        try:
            latencyList = []
            for i in self.hkey_list:
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.hdel, 'test_hash', i)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.hkey_list), latencyList, 'Hdels')
            print '%r subscribers hdel' % len(self.hkey_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'      
            
    def operZadd(self):
        try:
            latencyList = []
            for i in self.sorted_list:
                value = 'ScoreTestValue'+str(i)
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.zadd, 'test_score', i, value)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.sorted_list), latencyList, 'Zadds')
            print '%r subscribers zadd' % len(self.sorted_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'      
            
    def operZscore(self):
        try:
            latencyList = []
            for i in self.sorted_list:
                value = 'ScoreTestValue'+str(i)
                # how to check return score of self.resp_conn.zscore when used in the wrapper?
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.zscore, 'test_score', value)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.sorted_list), latencyList, 'Zscores')
            print '%r subscribers zscore' % len(self.sorted_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'      
            
    def operZrange(self):
        try:
            executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.zrange, 'test_score', 0, -1, withscores=True)
            latency = timeit.timeit(executeCommandWrapped, number=1)
            print 'The zrange latency is %r' % latency
            print '%r subscribers zrange' % len(self.sorted_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'      
            
    def operZrem(self):
        try:
            latencyList = []
            for i in self.sorted_list:
                self.resp_conn.zrem('test_score', i)
                executeCommandWrapped = self.executeCommandWrapper(self.resp_conn.zrem, 'test_score', i)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.sorted_list), latencyList, 'Zrems')
            print '%r subscribers zrem' % len(self.sorted_list)
        except redis.ResponseError:
            print 'Internal server error'
        except redis.ConnectionError:
            print 'Redis Connection Error, try again'      
