import sys
import ldap
import uuid
import random
import string
import abc
import ldap.modlist as modlist
import ldap.filter
from ldif import LDIFParser,LDIFWriter,LDIFRecordList
from base_oper import Operation
import threading
import time

class ParseLdif(LDIFParser):
    def __init__(self, input, output):
        LDIFParser.__init__(self,input)
        self.writer = LDIFWriter(output)
        
    def handle(self,dn,entry):
        self.writer.unparse(dn,entry)

class LdapOperation(Operation):
    'Common class for all LDAP operations'
    def __init__(self, ldap_uri='ldap://localhost:389', user='user1', pswd='pswd1', subs=100): #default constructor
        self.subs = subs
        self.ldap_uri = ldap_uri
        self.user = 'cn='+user
        self.pswd = pswd
        self.min_uid = 331020000000000
        self.max_uid = 331020000003630
        self.key_list = random.sample(xrange(self.min_uid, self.max_uid + 1), self.subs)
        #self.key_list = range(self.min_uid, self.max_uid + 1) #the whole range
        self.list_with_add_response_times = []
        self.list_with_search_response_times = []
        try:
            self.ldap_conn = ldap.initialize(self.ldap_uri, 0, sys.stdout, None)
            self.ldap_conn.bind_s(self.user, self.pswd, ldap.AUTH_SIMPLE)
        except ldap.SERVER_DOWN:
            print 'Server is down, try to start/restart it'

    def worker_add(self,dn,ldif):
        print "worker_add"
        time_before = time.time()
        ret = self.ldap_conn.add_s(dn,ldif)
        time_after = time.time()
        print "got response "+str(ret[0])+" sent in "+str(time_after-time_before)+" seconds"
        self.list_with_add_response_times.append(time_after-time_before)
        
    def worker_delete(self,dn):
        print "worker_delete"
        time_before = time.time()
        ret = self.ldap_conn.delete_s(dn)
        time_after = time.time()
        print "got response "+str(ret[0])+" sent in "+str(time_after-time_before)+" seconds"

    def worker_search(self, dn,searchScope, filter, attrs):
        print "worker_search"
        time_before = time.time()
        ret = self.ldap_conn.search(dn, searchScope, filter, attrs)
        time_after = time.time()
        print "got response "+str(ret)+" sent in "+str(time_after-time_before)+" seconds"
        self.list_with_search_response_times.append(time_after-time_before)


    def operAdd(self):
        threads_list = []
        print '\n start adding...'
        try:
            attrs = {}
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
                #self.ldap_conn.add_s(dn,ldif)
                t  = threading.Thread(target=self.worker_add, args=(dn,ldif))
                threads_list.append(t)
                t.start()
            for i in range(0,len(threads_list)):
                threads_list[i].join()
            print '\n%r subscribers added' % len(self.key_list)
            print "max response time is "+str(max(self.list_with_add_response_times))
            print self.key_list
            
        except ldap.ALREADY_EXISTS:
            print 'ForwardingError - keyAlreadyExists, addition stopped at %r entries' %uid
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, addition stopped at %r entries' %uid   

    def operModify(self, how_many):
        print '\n start modification...'
        modify_list = random.sample(self.key_list, how_many)
        old = {'givenName':'Bernd'}
        new = {'givenName':'JaneDoe'}
        try:
            for i in modify_list:
                if modify_list:
                    uid_str = 'uid='+str(i)
                    baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'DC=C-NTDB')
                    dn = string.join(baseDn, ',')
                    # Do the actual modification
                    ldif = modlist.modifyModlist(old,new)
                    self.ldap_conn.modify_s(dn,ldif)
            print '%r subscribers modified' % len(modify_list)
            print modify_list
            
        except ldap.NO_SUCH_OBJECT:
            print 'No such object--try again, modification stopped at %r entries' %i        
        except ldap.NO_SUCH_ATTRIBUTE:
            print 'No Such Attribute--try again, modification stopped at %r entries' %i
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, modification stopped at %r entries' %i     

    def operSearch(self, how_many):
        print '\n start search...'
        result_set = []
        threads_list = []
        searchScope = ldap.SCOPE_SUBTREE
        search_list = random.sample(self.key_list, how_many)
        try:
            for i in search_list:
                if search_list:
                    uid_str = 'uid='+str(i)
                    baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'DC=C-NTDB')
                    dn = string.join(baseDn, ',')
                    # If we want to generate more complicated filters on the fly
                    # or let the users specify the filter, we probably want to use
                    # the escape_filter_chars/filter_format methods to play it safe.
                    filter = 'uid='+str(i)
                    #filter = '((objectclass=subscriber)('+uid_str+'))'
                    attrs = ['uid', 'description','sn']
                    results = self.ldap_conn.search(dn, searchScope, filter, attrs)
                    result_type, result_data = self.ldap_conn.result(results, 0)
                    if result_type == ldap.RES_SEARCH_ENTRY:
                        result_set.append(result_data)
            print '\n%r subscribers searched' % len(search_list)
            print search_list
            
        except ldap.PROTOCOL_ERROR:
            print 'Failed to extract key from DN--try again, search stopped at %r entries' %i        
        except ldap.NO_SUCH_OBJECT:
            print 'No such object--try again, search stopped at %r entries' %i        
        except ldap.NO_SUCH_ATTRIBUTE:
            print 'No Such Attribute--try again, search stopped at %r entries' %i
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, search stopped at %r entries' %i   
        

    def operDelete(self,how_many):
        print '\n start deletion...'
        delete_list = random.sample(self.key_list, how_many)
        try:
            for i in delete_list:
                if delete_list:
                    uid_str = 'uid='+str(i)
                    baseDn = (uid_str, 'ds=SUBSCRIBER', 'o=DEFAULT', 'DC=C-NTDB')
                    dn = string.join(baseDn, ',')
                    self.ldap_conn.delete_s(dn)
                    self.key_list.remove(i)
            print '%r subscribers deleted' % len(delete_list)
            print delete_list
                    
        except ldap.NO_SUCH_OBJECT:
            print 'No such object--try again, deletion stopped at %r entries' %i
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, deletion stopped at %r entries' %i    
                
        return self.key_list
        
    def clearDB(self):
        print '\n start clearing DB...'
        count = 0 #how many are cleared
        print  self.key_list       
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
            print 'No such object--try again, clearing stopped at %r entries' %count
        except ldap.SERVER_DOWN:
            print 'cannot contact LDAP server, clearing stopped at %r entries' %count    
