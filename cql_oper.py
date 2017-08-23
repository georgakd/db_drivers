import sys
import random
import time
import timeit
import measurements


try:
    import cassandra
    from cassandra.cluster import Cluster
except ImportError, e:
    sys.exit('\nPython Cassandra driver not installed, or not on PYTHONPATH.\n'
             'You might try "pip install cassandra-driver".\n'
             'Python: %s\n'
             'Module load path: %r\n'
             'Error: %s\n' % (sys.executable, sys.path, e))
             
             
class CqlOperation(object):
    """Common class for all Cql Operations"""
    def __init__(self, host='localhost', port='9042', cql_version='3.0.0', subs=1, test_load='no'):
        self.host = host
        self.port = port        
        self.cql_version = cql_version
        self.subs = subs
        self.test_load = test_load
        self.min_uid = 460210000000001
        self.max_uid = 460210000100000
        self.key_list = random.sample(xrange(self.min_uid, self.max_uid + 1), self.subs)
        # Create a cluster of 1 node at 127.0.0.1
        self.cluster = Cluster()
        try:
            self.session = self.cluster.connect()
        except cassandra.cluster.NoHostAvailable:
            print 'Cql Connection Error, host is down, try again'

    def sessionExecuteWrapper(self, func, *args, **kwargs):
        """Wrapper function to transform session.execute(query) to session.execute() for using it in timeit.
           It is based on the FunctionWrapper design pattern."""
        def sessionExecuteWrapped():
            return func(*args, **kwargs)
        return sessionExecuteWrapped

    def validLoadOpts(self):
        load_list = ['insert', 'select', 'update', 'delete', 'mix', 'no']
        return self.test_load in load_list
        
    def chooseLoad(self, which_load):             
        if not self.validLoadOpts():
            print 'Unknown load...try again'
            raise SystemExit
        self.operCreateKeyspace()
        self.operCreateTable()	  # always create one Table

        start = time.time()
        print('Started measuring execution time.....')
        self.operInsert()         # insert once and for all!

        if which_load == 'insert':  # insert does ONLY INSERT new keys
            print 'I have already inserted, nothing more to do'
        if which_load == 'select':
            self.operSelectStar()
        if which_load == 'update':  # update does ONLY UPDATE existing keys
            self.operUpdate()
        if which_load == 'delete':
            self.operDelete()
        if which_load == 'mix':
            self.operSelectStar()
            self.operUpdate()
            self.operDelete()
        elif which_load == 'no':
            print ('No load used')
        end = time.time()
        print('Operations took %r sec' % (end - start))
        measurements.calcThroughput(len(self.key_list), (end - start))
                
    def operCreateKeyspace(self):
        try:
            self.session.execute('''CREATE KEYSPACE IF NOT EXISTS SDLKeyspace 
                                    WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};''')
        except cassandra.protocol.ConfigurationException:
            print 'Keyspace does not exist'

    def operCreateTable(self):
        """ Create a table once """
        try:
            self.session.execute('''CREATE TABLE IF NOT EXISTS SDLKeyspace.SUBSCRIBER (
                                 IMSI BIGINT, IMSI_LENGTH TINYINT, PARTITION_KEY BIGINT,
                                 MSISDN VARCHAR, IMEI VARCHAR, IMEI_SOFTWARE_VERSION VARCHAR, 
                                 IMSI_STRING VARCHAR, HSS_COMMON VARCHAR, HSS_APN_CONFIG VARCHAR, 
                                 RA_ACCESS_TYPE TINYINT, HSS_IDENTITY VARCHAR, MS_NETWORK_CAPA VARCHAR, 
                                 HSS_RESET_FLAG TINYINT, SUBS_IN_HSS_HLR TINYINT, PAPS_ID BLOB, 
                                 MME_CANCELLED TINYINT, SGSN_CANCELLED TINYINT, MTMSI BIGINT, 
                                 MME_S1AP_ID BIGINT, MME_TEID BIGINT, OLD_MTMSI BIGINT, 
                                 EMM_STATE TINYINT, ECM_STATE TINYINT, CPPS_ID TINYINT, 
                                 GLOBAL_ENB_ID BLOB, MME_BASE BLOB, MME_COMMON BLOB, 
                                 HSS_MME VARCHAR, MME_SXN VARCHAR, MME_BRR VARCHAR, 
                                 MME_SECURITY_CNTXT VARCHAR, MME_UE_RADIO_CAPA VARCHAR, MME_AUTH_VECTOR VARCHAR, 
                                 MSCVLR_NUM SMALLINT, MSCVLR_RELIABLE TINYINT, SGS_STATE TINYINT, 
                                 UELB_TIMESTAMP TIMESTAMP, UE_WAKEUP_TIME TIMESTAMP, UE_WAKEUP_REASON TINYINT, 
                                 DEF_BEARER_COUNT TINYINT, DED_BEARER_COUNT TINYINT, SGW_ID BIGINT, 
                                 SGW_RESTART_CNT SMALLINT, UE_CSFB_CAP TINYINT, UE_ACCESS_PRIORITY SMALLINT, 
                                 S102_LINK_IDX SMALLINT, S102_RESTART_CNT SMALLINT, MME_GBR_PRESENT TINYINT, 
                                 MME_NUM_EMER_BRR TINYINT, SGS_IDX SMALLINT, ENB_IDX BIGINT, 
                                 CIVIC_ADDRESS BLOB, PRIMARY KEY( IMSI));''')
        except cassandra.OperationTimedOut:
            print 'CQL has timed out for operation create table'

    def operInsert(self):
        """ Insert values at the table once """
        try:
            insertQueryCols = '''INSERT INTO SDLKeyspace.SUBSCRIBER (
                                     IMSI, IMSI_LENGTH, PARTITION_KEY,
                                     MSISDN, IMEI, IMEI_SOFTWARE_VERSION,
                                     IMSI_STRING, HSS_COMMON, HSS_APN_CONFIG,
                                     RA_ACCESS_TYPE, HSS_IDENTITY, MS_NETWORK_CAPA,
                                     HSS_RESET_FLAG, SUBS_IN_HSS_HLR, MME_CANCELLED,
                                     SGSN_CANCELLED, EMM_STATE, ECM_STATE,
                                     HSS_MME, MME_SXN, MME_BRR,
                                     MME_SECURITY_CNTXT, MME_UE_RADIO_CAPA, MME_AUTH_VECTOR,
                                     MSCVLR_NUM, MSCVLR_RELIABLE, SGS_STATE,
                                     UE_WAKEUP_REASON, DEF_BEARER_COUNT, DED_BEARER_COUNT,
                                     SGW_ID, SGW_RESTART_CNT, UE_CSFB_CAP, 
                                     UE_ACCESS_PRIORITY, S102_LINK_IDX, S102_RESTART_CNT, 
                                     MME_GBR_PRESENT, MME_NUM_EMER_BRR, SGS_IDX, ENB_IDX 
                                     )
                                     VALUES ('''
            insertQueryVals = ''', 1, 41188,
                                     '358610000000001', '00000000000000', '00',
                                     '460210000000001', 'deletingEntry: False', '[]',
                                     3, 'HSS.ttcn3', '0xA541C0',
                                     0, 1, 0,
                                     1, 2, 2,
                                     '[]', '[]', '[]',
                                     'airSeqNum: 0', '0', 'curAuthIdx: 0',
                                     0, 0, 0,
                                     0, 0, 0,
                                     1, 0, 0,
                                     0, 0, 0,
                                     0, 0, 0, 1
                                     );'''
            insertLatencyList = []
            for i in self.key_list:
                primaryKey = str(i)
                query = insertQueryCols + primaryKey + insertQueryVals
                sessionExecuteWrapped = self.sessionExecuteWrapper(self.session.execute, query)
                insertLatency = timeit.timeit(sessionExecuteWrapped, number=1)
                insertLatencyList.append(insertLatency)
            measurements.plotLatency(len(self.key_list), insertLatencyList, 'Inserts')
            print '%r subscribers inserted' % len(self.key_list)
        except cassandra.OperationTimedOut:
            print 'CQL has timed out for operation insert'

    def operSelectStar(self):
        try:
            selectStarQuery = '''SELECT * FROM SDLKeyspace.SUBSCRIBER WHERE IMSI='''

            selectLatencyList = []
            for i in self.key_list:
                primaryKey = str(i)
                query = selectStarQuery + primaryKey + ';'
                sessionExecuteWrapped = self.sessionExecuteWrapper(self.session.execute, query)
                selectLatency = timeit.timeit(sessionExecuteWrapped, number=1)
                selectLatencyList.append(selectLatency)
            measurements.plotLatency(len(self.key_list), selectLatencyList, 'Selects')
            print '%r subscribers selected' % len(self.key_list)
        except cassandra.OperationTimedOut:
            print 'CQL has timed out for operation select'

    def operUpdate(self):
        try:
            updateQuery = '''UPDATE SDLKeyspace.SUBSCRIBER SET MSISDN = '358610000000006' WHERE IMSI ='''

            updateLatencyList = []
            for i in self.key_list:
                primaryKey = str(i)
                query = updateQuery + primaryKey + ';'
                sessionExecuteWrapped = self.sessionExecuteWrapper(self.session.execute, query)
                updateLatency = timeit.timeit(sessionExecuteWrapped, number=1)
                updateLatencyList.append(updateLatency)
            measurements.plotLatency(len(self.key_list), updateLatencyList, 'Updates')
            print '%r subscribers updated' % len(self.key_list)
        except cassandra.OperationTimedOut:
            print 'CQL has timed out for operation update'

    def operDelete(self):
        try:
            deleteQuery = 'DELETE FROM SDLKeyspace.SUBSCRIBER  WHERE IMSI ='

            deleteLatencyList = []
            for i in self.key_list:
                primaryKey = str(i)
                query = deleteQuery + primaryKey + ';'
                sessionExecuteWrapped = self.sessionExecuteWrapper(self.session.execute, query)
                deleteLatency = timeit.timeit(sessionExecuteWrapped, number=1)
                deleteLatencyList.append(deleteLatency)
            measurements.plotLatency(len(self.key_list), deleteLatencyList, 'Deletes')
            print '%r subscribers deleted' % len(self.key_list)
        except cassandra.OperationTimedOut:
            print 'CQL has timed out for operation delete'
