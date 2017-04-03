#!/usr/bin/python


import subprocess
import sys
import datetime
import socket

monitor_output_filename = 'monitor_stop.log'


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


subprocess.call('pkill dstat',shell=True) # ignore return status

execute_str('df -h')
execute_str('free -th')

end_timestamp = get_timestamp()
emit ("end time: %s\n"%end_timestamp)
