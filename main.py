#!/usr/bin/env python
# -*- coding: utf-8 -*-
# *********************************************************************************************************************

import subprocess
import os
import argparse
import ldap_oper
import resp_oper
import cql_oper
from sys import argv
from os.path import exists

parser = argparse.ArgumentParser(description='Benchmarking DBs')
parser.add_argument('interface', help='Pick between ldap, resp and cql')
parser.add_argument('load', help='Pick a load (no=without load). '
                                 '|LDAP: add modify search delete mix no '
                                 '|RESP: set get delete hset hgetall hdel zadd zscore zrange zrem no '
                                 '|CQL: insert select update delete mix no')
parser.add_argument('subscribers', help='Give an integer number for subscribers')
args = parser.parse_args()
script, interface, load, subscribers = argv


def validInterfaceOpts():
    interface_list = ['ldap', 'resp', 'cql']
    return interface in interface_list

def main():
    # Check if interface valid
    if not validInterfaceOpts():
        print 'Unknown interface...try again'
        raise SystemExit

    if interface == 'ldap':
        Load = ldap_oper.LdapOperation(subs=int(subscribers), test_load=load)
        Load.chooseLoad(Load.test_load)
    elif interface == 'resp':                
        Load = resp_oper.RespOperation(subs=int(subscribers), test_load=load)
        Load.chooseLoad(Load.test_load)
    elif interface == 'cql':
        Load = cql_oper.CqlOperation(subs=int(subscribers), test_load=load)
        Load.chooseLoad(Load.test_load)
        
if __name__ == "__main__":
    main()