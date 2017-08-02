# Pre-requisites to run the tools
- openldap tools -> yum install openldap
- python -> ensure that python is installed in /usr/bin/python
- python-ldap module -> pip install python-ldap
- redis module-> pip install redis
- cassandra module-> pip install cassandra-driver
- numpy-> pip install numpy
- matplotlib-> yum install python-matplotlib

Note that you must also have ldap and/or redis and/or cassandra servers up and running.

# Usage of the database drivers
- Run the main script and see the help menu: ./main.py -h
- Run the script with the options of your choice: 
  - interface: Pick between ldap, resp and cql
  - load: Pick a load (no=without load). LDAP: add modify search delete mix no| 
                                         RESP: set get delete hset hgetall hdel zadd zscore zrange zrem no| 
                                         CQL: insert select update delete mix no
                                         
  - subscribers: Give an integer number for subscribers (from 1 - 99.999)
- Latency graphs are produced

# TODO
- [ ] Calculate and output graph throughput
- [ ] Use threads to simulate multiple parallel clients
- [ ] Add sqlite3 driver

 
