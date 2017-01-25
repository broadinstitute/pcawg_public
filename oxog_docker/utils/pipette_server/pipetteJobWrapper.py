#!/usr/bin/env python




import os
import subprocess
import sys
import datetime
import shutil
import tempfile
import resource
import collections
import socket
import errno




def complain(msg):
    sys.stderr.write(msg)
    sys.exit(1)


def write_msg_to_file(filepath,msg):
    fid = open(filepath,'wt')
    fid.write(msg+'\n')
    fid.close()

def read_msg_from_file(filepath):
    if not os.path.exists(filepath):
        return 'Empty'
    fid = open(filepath,'rt')
    msg = fid.read()
    fid.close()
    msg = msg.strip()
    return msg



token_counter = 0
last_ts = ''
def get_unique_token():
    global last_ts,token_counter
    #get a timestamp string, ordered years to seconds
    ts = get_timestamp()

    # set token counter to make timestamp unique within this instance of the pipette client.
    if ts == last_ts:
        token_counter = token_counter+1
    else:
        token_counter = 0
    last_ts = ts

    #set random 6 character string, for easier human readability.
    uniq_hash = tempfile.mktemp(suffix='', prefix='', dir='')
    uniq_hash = uniq_hash.replace('-','_')

    #create unique string that sorts in order of creation
    try:
        #Python 3 version
        uniq_str = '{}.{:03d}.{}'.format(ts,token_counter,uniq_hash)
    except:
        #Python 2 version
        uniq_str = '%s.%03d.%s'%(ts,token_counter,uniq_hash)


    return uniq_str


def get_timestamp():
    t=datetime.datetime.now()
    try:
        #Python 3 version
        timestamp='{0.year:d}_{0.month:02d}_{0.day:02d}__{0.hour:02d}_{0.minute:02d}_{0.second:02d}'.format(t)
    except:
        #Python 2 version
        timestamp = '%d_%02d_%02d__%02d_%02d_%02d'%(t.year,t.month,t.day,t.hour,t.minute,t.second)
    return timestamp


def get_timestamp_delta(ts_begin,ts_end):
    begin_secs = timestamp_to_seconds(ts_begin)
    end_secs = timestamp_to_seconds(ts_end)
    duration = end_secs - begin_secs
    return duration

def timestamp_to_seconds(timestamp):
    (year,month,day,junk,hour,minute,second)= timestamp.split('_')
    (year,month,day,junk,hour,minute,second) = (
        int(year),int(month),int(day),junk,int(hour),int(minute),int(second))

    dt = datetime.datetime(year,month,day,hour,minute,second)
    days = dt.toordinal()
    days = days - 733000 # keep the seconds to a managable size...
    secs = days*24*60*60 + hour*60*60 + minute*60 + second
    return secs


if __name__ == '__main__':

    # Get input args
    # 1st argument - output directory - cwd
    base_outdir = sys.argv[1]
    maxmem = sys.argv[2]
    maxtime = sys.argv[3]
    do_retry_arg = sys.argv[4]
    # 4th argument - the name of the task and its argument - joined together with spaces.
    cmd_str_arg = ' '.join(sys.argv[5:])

    # Error checks
    if not os.path.exists(base_outdir):
        complain('Output directory does not exist: '+base_outdir)

    if not os.path.isabs(base_outdir):
        complain('Output directory must be a full path')

    if 'ipette' not in base_outdir:
        complain('Output directory must contain pipette or Pipette, for safety')

        #if not '$MODULEOUTDIR' in cmd_str_arg:
        #complain('Could not find $MODULEOUTDIR string in cmdStr: '+cmd_str_arg)


    # Create some handy strings

    status_filepath = os.path.join(base_outdir,'pipette.engine_retrydir_status.txt')
    engine_dir_prepender = 'pipette.engine_outdir.'
    runId =  engine_dir_prepender + get_unique_token()


    if do_retry_arg == 'True':
        do_retry = True
    elif do_retry_arg == 'False':
        do_retry = False
    else:
        raise Exception('unknown value for do_retry, must be True or False: %s'%do_retry_arg)

    if do_retry:
        job_outdir = os.path.join(base_outdir,runId)
    else:
        job_outdir = base_outdir



    # Check what the state of the directory is
    status = read_msg_from_file(status_filepath)
    if status == 'Empty':
        # status file is missing
        # check if there are other files here
        dir_contents = os.listdir(base_outdir)
        for okfn in ['pipette.wrapper.stdout.txt','pipette.wrapper.stderr.txt','jobSubmissions.txt']:
            if okfn in dir_contents:
                dir_contents.remove(okfn)
        okfn = 'pipette.module_retrydir_status.txt'
        if not do_retry and okfn in dir_contents:
            dir_contents.remove(okfn)
        if len(dir_contents)!=0:
            complain('outdir must start out empty or only have stuff left from previous runs of this rerunnable wrapper script: '+str(dir_contents))
        else:
            # starting with clean output directory
            pass
    elif status in ['Pass','AboutToPass']:
        # remove clutter from previous run - do an 'rm -r'
        shutil.rmtree(base_outdir)
        os.makedirs(base_outdir)
    elif status in ['Fail']:
        # clutter from old jobs will not bother us
        pass
    else:
        # another job is running... yield to it if it was started later than me.
        if status > runId:
            # this scenario seems unlikely - we just started!
            complain('Another job is running in the same directory with a later runId.\n'+
                     'existing runId: '+status+'\n'+
                     'attempted new runId: '+runId)
        else:
            # Another job started sometime in the past, but we will clobber it
            pass

    # We now have a sufficiently clean directory, and are free to move on
    # Log that we are here
    write_msg_to_file(status_filepath,runId)
    # Create the subdir that the command will actually use

    try:
        os.makedirs(job_outdir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


    #removed sleep

    # Execute the command
    # Should stdout/stderr go to the parent, or to these separate files?
    cmd_str = cmd_str_arg.replace('$MODULEOUTDIR',job_outdir)
    stdout_file_path = os.path.join(job_outdir,'pipette.module.stdout.txt')
    stderr_file_path = os.path.join(job_outdir,'pipette.module.stderr.txt')
    cmd_file_path = os.path.join(job_outdir,'pipette.module.cmd.txt')
    usage_file_path = os.path.join(job_outdir,'pipette.module.usage.txt')


    stdout_file = open(stdout_file_path,'wt')
    stderr_file = open(stderr_file_path,'wt')

    print ('About to call '+cmd_str)
    cmd_msg = 'cd ' + job_outdir + '\n\n' + cmd_str
    write_msg_to_file(cmd_file_path, cmd_msg)

    start_timestamp = get_timestamp()

    # waits for the command to complete
    exit_code = subprocess.call(cmd_str,shell=True, \
                                bufsize=-1,cwd=job_outdir,
                                stdout = stdout_file,stderr = stderr_file)

    end_timestamp = get_timestamp()
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    if exit_code == 0:
        passfail = "pass"
    else:
        passfail = "FAIL"
    maxrss_memory = float(usage.ru_maxrss)/(2**20)  #convert memory to GB


    usage_dict = collections.OrderedDict()
    if do_retry:
        job_outdir_list = job_outdir.split('/')
        final_outdir = '/'.join(job_outdir_list[:-2])
    else:
        final_outdir = job_outdir
    usage_dict['#status'] = passfail
    usage_dict['exit_code'] = str(exit_code)
    usage_dict['final_outdir'] = final_outdir
    usage_dict['start_time'] = start_timestamp
    usage_dict['end_time'] = end_timestamp
    usage_dict['wallclock_duration_s'] = str(get_timestamp_delta(start_timestamp, end_timestamp))
    usage_dict['user_cputime_s'] = '%7.3f'%usage.ru_utime
    usage_dict['system_cputime_s'] = '%7.3f'%usage.ru_stime
    usage_dict['maxtime'] = maxtime
    #note maxrss may just capture the real memory used by the largest child, not all the children
    usage_dict['maxrss_memory_gb'] = '%7.3f'%maxrss_memory
    usage_dict['maxmem'] = maxmem
    usage_dict['page_faults'] = '%d'%usage.ru_majflt
    usage_dict['node_ip_addr'] = socket.gethostbyname(socket.gethostname())

    usage_str = '\t'.join(usage_dict.keys()) + '\n'
    usage_str += '\t'.join(usage_dict.values())
    write_msg_to_file(usage_file_path,usage_str)


    if exit_code == 140:
        print ('Job killed because it hit a resource limit - please increase maxtime.')


    stdout_file.close()
    stderr_file.close()


    # Command has finished.
    # Check the status again, to see if anyone has changed it out from under us.
    status = read_msg_from_file(status_filepath)

    if status != runId or exit_code != 0:

        # Something bad must have happened.
        if status == runId:
            # No other job was running, we just failed.
            # return the job's exit code
            write_msg_to_file(status_filepath,'Fail')
            sys.exit(exit_code)
        else:
            # Some other job must have clobbered us.
            # Yield the floor to it.
            complain('Another job started running in the same directory after this job started, exiting to avoid clobbering it.\n'+
                     'existing runId: '+status+'\n'+
                     'attempted new runId: '+runId+'\n'+
                     'original exit code = '+str(exit_code))


    # Looking good... commence the passing procedure
    # Log that we are about to make a bunch of changes...        
    write_msg_to_file(status_filepath,'AboutToPass')

    if do_retry:
        basedir_contents = os.listdir(base_outdir)
        job_subdir_contents = os.listdir(job_outdir)
        # move the passing output directory to its parent directory
        try:
            for fn in job_subdir_contents:
                oldpath = os.path.join(job_outdir,fn)
                newpath = os.path.join(base_outdir,fn)
                os.rename(oldpath,newpath)
        except:
            complain('failed to move contents of passing directory to its parent')
        # purge any old failing output directories, plus the now-empty passing directory
        try:
            for fn in basedir_contents:
                if fn.startswith(engine_dir_prepender):
                    goner_dir = os.path.join(base_outdir,fn)
                    shutil.rmtree(goner_dir)
        except:
            complain('failed to remove failing directories')
    # Double-check that we are the only ones active in this directory
    status = read_msg_from_file(status_filepath)

    if status != 'AboutToPass':
        # Another process has tromped on things... let them.
        complain('Another job interrupted the process of cleaning up this passing job')

    write_msg_to_file(status_filepath,'Pass')

    # Now we are done.
    sys.exit(0)

        
