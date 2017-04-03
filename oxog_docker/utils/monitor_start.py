#!/usr/bin/python


import subprocess
import sys
import datetime
import socket

monitor_output_filename = 'monitor_start.log'
dstat_log_filename = 'dstat.log'
dstat_full_log_filename = 'dstat_full.log'

def execute_str(cmd_str):

    p = subprocess.Popen(cmd_str,shell=True,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
    (stdout,stderr) = p.communicate()
    return_code = p.returncode
    err = return_code!=0
    try: #python3
        stdout_str = str(stdout,'utf-8')
        stderr_str = str(stderr,'utf-8')
    except: #python2 fallback
        stdout_str = str(stdout)
        stderr_str = str(stderr)

    out_str = '%s\n%s\n\n'%(cmd_str,stdout_str)
    emit(out_str)

    return (err,stdout_str,stderr_str)

def emit(msg):
    #print(msg)
    append_msg_to_file(monitor_output_filename,msg)


def append_msg_to_file(filepath,msg):
    fid = open(filepath,'a')
    fid.write(msg)
    fid.close()


def get_timestamp():
    t=datetime.datetime.now()
    try:
        #Python 3 version
        timestamp='{0.year:d}_{0.month:02d}_{0.day:02d}__{0.hour:02d}_{0.minute:02d}_{0.second:02d}'.format(t)
    except:
        #Python 2 version
        timestamp = '%d_%02d_%02d__%02d_%02d_%02d'%(t.year,t.month,t.day,t.hour,t.minute,t.second)
    return timestamp



cmd_str = ' '.join(sys.argv[1:])




execute_str('whoami')
execute_str('uname -a')
execute_str('df -h')
execute_str('echo num processors: `grep -c ^processor /proc/cpuinfo`')
execute_str('free -th')

execute_str('pwd')

execute_str('curl "http://metadata.google.internal/computeMetadata/v1/project/project-id" -H "Metadata-Flavor: Google"')
execute_str('curl "http://metadata.google.internal/computeMetadata/v1/instance/zone" -H "Metadata-Flavor: Google"')
execute_str('curl "http://metadata.google.internal/computeMetadata/v1/instance/machine-type" -H "Metadata-Flavor: Google"')
execute_str('curl "http://metadata.google.internal/computeMetadata/v1/instance/hostname" -H "Metadata-Flavor: Google"')
execute_str('curl "http://metadata.google.internal/computeMetadata/v1/instance/scheduling/preemptible" -H "Metadata-Flavor: Google"')
execute_str('curl "http://metadata.google.internal/computeMetadata/v1/instance/description" -H "Metadata-Flavor: Google"')

(err,stdout_str16, stderr_str) = execute_str('curl "http://metadata.google.internal/computeMetadata/v1/instance/disks/" -H "Metadata-Flavor: Google" | wc -l')
num_disks = int(stdout_str16)
disk_info = ''
for d in range(num_disks):
    (err, stdout_str17, stderr_str) = execute_str(
        'curl "http://metadata.google.internal/computeMetadata/v1/instance/disks/%d/type" -H "Metadata-Flavor: Google" '%d)
    info = 'disk %d: %s; '%(d,stdout_str17)
    disk_info += info
emit (disk_info)

start_timestamp = get_timestamp()
emit ("start: %s"%start_timestamp)
ip_address = socket.gethostbyname(socket.gethostname())
emit("ip address: %s"%ip_address)

# create dev/null object
try:
    from subprocess import DEVNULL # py3k
except ImportError:
    import os
    DEVNULL = open(os.devnull, 'w')
# launch dstat as a background process
dstat_cmd_str = "dstat --nocolor --noheaders -tcdngym --freespace --output %s"%dstat_log_filename
dstat_list=dstat_cmd_str.split()
dstat_process_mini = subprocess.Popen(dstat_list, stdout=DEVNULL, stderr=subprocess.STDOUT)

# launch dstat full as a background process, routing the stdout to /dev/null

dstat_cmd_str = "dstat --nocolor --noheaders -tcdngym --freespace --top-cpu-adv --top-io --top-mem --top-bio-adv --dstat-mem  --output %s"%dstat_full_log_filename
dstat_list=dstat_cmd_str.split()
dstat_process_full = subprocess.Popen(dstat_list, stdout=DEVNULL, stderr=subprocess.STDOUT)



