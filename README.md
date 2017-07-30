# Pre-requisites to run the tools
- valgrind -> yum install valgrind
- kcachegrind -> yum install kcachegrind
- openldap tools -> yum install openldap
- python -> ensure that python is installed in /usr/bin/python
- python-ldap module -> pip install python-ldap
- redis module-> pip install redis
- cassandra module-> pip install cassandra-driver

# Valgrind for memory leaks (memcheck)
In order to find the memory leaks for a process, you will need to perform the following steps:
- Ensure that service is not running
- Modify valgrind.conf with the process you want to valgrind, e.g. storage  
- Run the shell script: sudo ./sdlValgrind.sh valgrind.conf --start --output=text (or --output=xml)
- When the memcheck monitoring is finished run: sudo ./sdlValgrind.sh valgrind.conf --stop
- The results are available in valgrind folder

# Valgrind for performance testing (callgrind)
In order to produce a callgrind graph for a process, you will need to perform the following steps:
- Ensure that service is not running
- Run the python script and see the help menu:  sudo ./SDL_Valgrind.py -h
- Run the script with the options of your choice: 
  - interface: Pick between ldap, resp and cql
  - process: Pick a process to profile
  - tool: Pick a perf tool (no=without profiling) memcheck massif cachegrind callgrind helgrind perf no
  - load: Pick a load (no=without load). LDAP: add modify search delete mix no| 
                                         RESP: set get delete hset hgetall hdel zadd zscore zrange zrem no| 
                                         CQL: insert select update delete mix no
                                         
  - subscribers: Give an integer number for subscribers (from 1 - 99.999)
- Service is stopped automatically after the end of the script and results are available in valgrind folder
- Open tha callgrind output: sudo kcachegrind valgrind/callgrind.out.*

 
