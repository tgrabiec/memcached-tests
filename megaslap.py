#!/usr/bin/env python3
import subprocess
import argparse
import os
import sys
import math
import re
from collections import defaultdict

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', '-s', action="store", help="server adddress in <host>:<port> format", default="localhost:11211")
    parser.add_argument('--udp', '-U', action="store_true", help="use UDP protocol")
    parser.add_argument('-n', '--instances', action="store", help="instance count", default=4, type=int)
    parser.add_argument('-c', '--concurrency', action="store", help="concurrency level", default=128, type=int)
    parser.add_argument('-t', '--duration', action="store", help="test duration", default='10s')
    args = parser.parse_args()

    per_instance_concurrency = (args.concurrency + args.instances - 1) // args.instances
    if per_instance_concurrency * args.instances != int(args.concurrency):
        print('WARNING: total concurrency was rounded up to %d so that it divides by %d instances' % (
            per_instance_concurrency * args.instances, args.instances))

    slaves = []
    logs = []
    for i in range(args.instances):
        log_file = 'megaslap.log.%d' % i
        logs.append(log_file)
        memaslap_args = '-s ' + args.server
        memaslap_args += ' -c ' + str(args.concurrency)
        memaslap_args += ' -t ' + args.duration
        if args.udp:
            memaslap_args += ' -U'
        p = subprocess.Popen(['taskset -ca %d memaslap %s > %s' % (i, memaslap_args, log_file)], shell=True)
        slaves.append(p)

    for slave in slaves:
        slave.wait()
        if slave.returncode:
            raise Exception('One or more instances failed')

    pattern = r"""servers : .*
threads count: .*
concurrency: .*
run time: .*
windows size: .*
set proportion: set_prop=.*
get proportion: get_prop=.*
cmd_get: (?P<cmd_get>\d+)
cmd_set: (?P<cmd_set>\d+)
get_misses: (?P<get_misses>\d+)
written_bytes: (?P<written_bytes>\d+)
read_bytes: (?P<read_bytes>\d+)
object_bytes: .*(
packet_disorder: (?P<packet_disorder>\d+)
packet_drop: (?P<packet_drop>\d+)
udp_timeout: (?P<udp_timeout>\d+))?

Run time: .*? Ops: (?P<ops>\d+) TPS: (?P<tps>\d+) Net_rate: (?P<net_rate>[\d.]+)M/s"""

    props = defaultdict(float)

    for log in logs:
        with open(log, 'r') as file:
            m = re.match(pattern, file.read(), re.MULTILINE)
            if not m:
                print('Content of file %s does not match expected format' % (log))
                sys.exit(1)
            for k, v in m.groupdict().items():
                if v:
                    props[k] += float(v)

    for name in ['cmd_get', 'cmd_set', 'get_misses', 'written_bytes', 'read_bytes',
                    'packet_drop', 'packet_disorder', 'udp_timeout', 'ops', 'tps']:
        if name in props:
            print('%s: %d' % (name, props[name]))

    print('net_rate: %f.2 M/s' % props['net_rate'])
