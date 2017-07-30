import sys
import random
import string
import time
import timeit
import measurements
import threading

from base_oper import Operation
try:
    import ldap
    import ldap.filter
    import ldap.modlist as modlist 
except ImportError, e:
    sys.exit('\nPython Ldap driver not installed, or not on PYTHONPATH.\n'
             'You might try "yum install openldap" and "pip install python-ldap".\n'
             'Python: %s\n'
             'Module load path: %r\n'
             'Error: %s\n' % (sys.executable, sys.path, e))


class LdapOperation(Operation):
    """Common class for all LDAP operations"""
    def __init__(self, ldap_uri='ldap://localhost:389', user='sdl', pswd='sdl', subs=10, test_load='no'):
        self.ldap_uri = ldap_uri
        self.user = 'cn='+user
        self.pswd = pswd
        self.subs = subs
        self.test_load = test_load
        self.min_uid = 331020000000001
        self.max_uid = 331020000100000
        self.key_list = random.sample(xrange(self.min_uid, self.max_uid + 1), self.subs)
        self.list_with_add_response_times = []
        self.list_with_search_response_times = []
        try:
            self.ldap_conn = ldap.initialize(self.ldap_uri, 0, sys.stdout, None)
            self.ldap_conn.bind_s(self.user, self.pswd, ldap.AUTH_SIMPLE)
        except ldap.SERVER_DOWN:
            print 'Server is down, try to start/restart it'

    def executeCommandWrapper(self, func, *args, **kwargs):
        """Wrapper function to transform ldap commands with args to commands with no args for using it in timeit.
           It is based on the FunctionWrapper design pattern."""
        def executeCommandWrapped():
            return func(*args, **kwargs)
        return executeCommandWrapped
    
    def validLoadOpts(self):
        load_list = ['add', 'modify', 'search', 'delete', 'mix', 'no']
        return self.test_load in load_list
        
    def chooseLoad(self, which_load):             
        if not self.validLoadOpts():
            print 'Unknown load...try again'
            raise SystemExit
            
        start = time.time()
        print('Started measuring execution time.....')
        self.operAdd()	
        if which_load == 'add':
            print 'Subscribers provisioned, no more operations.'     
        elif which_load == 'modify':
            uidsToModify = (50*self.subs)/100
            self.operModify(uidsToModify)
        elif which_load == 'search':
            uidsToSearch = (50*self.subs)/100
            self.operSearch(uidsToSearch)
        elif which_load == 'delete':
            uidsToDelete = (50*self.subs)/100
            self.operDelete(uidsToDelete)
        elif which_load == 'mix':
            uidsToModify = (40*self.subs)/100
            uidsToSearch = (40*self.subs)/100
            uidsToDelete = (20*self.subs)/100        
            self.operModify(uidsToModify)
            self.operSearch(uidsToSearch)
            self.operDelete(uidsToDelete)
        elif which_load == 'no':
            print ('No load used')
        end = time.time()
        print('Operations took %r sec' % (end - start))
    
    def worker_add(self, dn, ldif):
        print "worker_add"
        time_before = time.time()
        ret = self.ldap_conn.add_s(dn, ldif)
        time_after = time.time()
        print "got response "+str(ret[0])+" sent in "+str(time_after-time_before)+" seconds"
        self.list_with_add_response_times.append(time_after-time_before)
        
    def worker_delete(self, dn):
        print "worker_delete"
        time_before = time.time()
        ret = self.ldap_conn.delete_s(dn)
        time_after = time.time()
        print "got response "+str(ret[0])+" sent in "+str(time_after-time_before)+" seconds"

    def worker_search(self, dn, searchScope, my_filter, attrs):
        print "worker_search"
        time_before = time.time()
        ret = self.ldap_conn.search(dn, searchScope, my_filter, attrs)
        time_after = time.time()
        print "got response "+str(ret)+" sent in "+str(time_after-time_before)+" seconds"
        self.list_with_search_response_times.append(time_after-time_before)

    def operAdd(self):
        threads_list = []
        print '\n start adding...'
        try:
            attrs = {}
            latencyList = []
            for uid in self.key_list:
                uid_str = 'uid='+str(uid)
                baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'DC=C-NTDB')
                dn = string.join(baseDn, ',')
                # A dict to help build the ldif 
                attrs['objectclass'] = ['subscriber']
                attrs['uid'] = [str(uid)]
                attrs['description'] = ['The Nameless Hero, the chosen one, the first of its kind.']
                attrs['givenName'] = ['Bernd']
                # Do the actual add 
                ldif = modlist.addModlist(attrs)
                executeCommandWrapped = self.executeCommandWrapper(self.ldap_conn.add_s, dn, ldif)
                latency = timeit.timeit(executeCommandWrapped, number=1)
                latencyList.append(latency)
            measurements.plotLatency(len(self.key_list), latencyList, 'LDAP ADDS')
                # t  = threading.Thread(target=self.worker_add, args=(dn, ldif))
                # threads_list.append(t)
                # t.start()
            # for i in range(0,len(threads_list)):
            #    threads_list[i].join()
            print '\n%r subscribers added' % len(self.key_list)
            # print "max response time is "+str(max(self.list_with_add_response_times))
            
        except ldap.ALREADY_EXISTS:
            print 'ForwardingError - keyAlreadyExists, addition stopped at %r entries' % uid
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, addition stopped at %r entries' % uid

    def operModify(self, how_many):
        print '\n start modification...'
        modify_list = random.sample(self.key_list, how_many)
        old = {'givenName': 'Bernd'}
        new = {'givenName': 'JaneDoe'}
        try:
            latencyList = []
            for i in modify_list:
                if modify_list:
                    uid_str = 'uid='+str(i)
                    baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'DC=C-NTDB')
                    dn = string.join(baseDn, ',')
                    # Do the actual modification
                    ldif = modlist.modifyModlist(old, new)
                    executeCommandWrapped = self.executeCommandWrapper(self.ldap_conn.modify_s, dn, ldif)
                    latency = timeit.timeit(executeCommandWrapped, number=1)
                    latencyList.append(latency)
            measurements.plotLatency(len(modify_list), latencyList, 'LDAP MODIFIES')
            print '%r subscribers modified' % len(modify_list)
            
        except ldap.NO_SUCH_OBJECT:
            print 'No such object--try again, modification stopped at %r entries' % i
        except ldap.NO_SUCH_ATTRIBUTE:
            print 'No Such Attribute--try again, modification stopped at %r entries' % i
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, modification stopped at %r entries' % i

    def operSearch(self, how_many):
        print '\n start search...'
        result_set = []
        threads_list = []
        searchScope = ldap.SCOPE_SUBTREE
        search_list = random.sample(self.key_list, how_many)
        try:
            latencyList = []
            for i in search_list:
                if search_list:
                    uid_str = 'uid='+str(i)
                    baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'DC=C-NTDB')
                    dn = string.join(baseDn, ',')
                    # If we want to generate more complicated filters on the fly
                    # or let the users specify the filter, we probably want to use
                    # the escape_filter_chars/filter_format methods to play it safe.
                    my_filter = 'uid='+str(i)
                    attrs = ['uid', 'description', 'sn']
                    executeCommandWrapped = self.executeCommandWrapper(self.ldap_conn.search, dn, searchScope,
                                                                       my_filter, attrs)
                    latency = timeit.timeit(executeCommandWrapped, number=1)
                    latencyList.append(latency)
            measurements.plotLatency(len(search_list), latencyList, 'LDAP SEARCHES')
            print '\n%r subscribers searched' % len(search_list)
            
        except ldap.PROTOCOL_ERROR:
            print 'Failed to extract key from DN--try again, search stopped at %r entries' % i
        except ldap.NO_SUCH_OBJECT:
            print 'No such object--try again, search stopped at %r entries' % i
        except ldap.NO_SUCH_ATTRIBUTE:
            print 'No Such Attribute--try again, search stopped at %r entries' % i
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, search stopped at %r entries' % i

    def operDelete(self, how_many):
        print '\n start deletion...'
        delete_list = random.sample(self.key_list, how_many)
        try:
            latencyList = []
            for i in delete_list:
                if delete_list:
                    uid_str = 'uid='+str(i)
                    baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'DC=C-NTDB')
                    dn = string.join(baseDn, ',')
                    self.ldap_conn.delete_s(dn)
                    executeCommandWrapped = self.executeCommandWrapper(self.ldap_conn.delete, dn)
                    latency = timeit.timeit(executeCommandWrapped, number=1)
                    latencyList.append(latency)
                    self.key_list.remove(i)
            measurements.plotLatency(len(delete_list), latencyList, 'LDAP DELETES')
            print '%r subscribers deleted' % len(delete_list)
                    
        except ldap.NO_SUCH_OBJECT:
            print 'No such object--try again, deletion stopped at %r entries' % i
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, deletion stopped at %r entries' % i
                
        return self.key_list
        
    def clearDB(self):
        print '\n start clearing DB...'
        count = 0  # how many are cleared
        try:
            for i in self.key_list:
                if self.key_list:
                    uid_str = 'uid='+str(i)
                    baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'dc=C-NTDB')
                    dn = string.join(baseDn, ',')
                    self.ldap_conn.delete_s(dn)
                    count = count + 1    
            print '%r subscribers cleared' % count
                
        except ldap.NO_SUCH_OBJECT:
            print 'No such object--try again, clearing stopped at %r entries' % count
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, clearing stopped at %r entries' % count
