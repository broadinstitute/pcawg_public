

import subprocess
import os
import getpass
import time

class LocalEngine:
    
    def __init__(self):        
        self._pipelineName_by_jobId = {}
        self._cmdStr_by_jobId = {}
        self._resources_by_jobId = {}
        self._job_outdir_by_jobId = {}
        self._localStatus_by_jobId = {}
        self._running_processes_by_jobId = {}
        self._filehandles_by_jobId = {}
        try:
            self._user = getpass.getuser()
        except KeyError:
            self._user = "unknown"

        self._firehose_mode = self._user.startswith('cgaadm')
        
        (cpu_count, mem_gb) = self._get_host_resources()
        self._numcores = cpu_count
        self._totmem = mem_gb
        print ('Local host has ' + str(cpu_count) + ' cores and ' + str(mem_gb) + 'GB.')


    def _max_jobs(self):
        #Set to some fraction of the number of cores.
        if self._firehose_mode:
            value = 1
        elif 'GALAXY_SLOTS' in os.environ:
            value = int(os.environ['GALAXY_SLOTS'])
        else:
            value = int(1.0 * self._numcores) #100 percent of cores are used.
            if value > 6:
                value = value - 2 #allow some cores for system processes
        return value
        

    def _max_mem(self):
        #Set to some fraction of the total memory, in GB.
        if self._firehose_mode:
            value = 1
        else:
            value = int(0.9*self._totmem) #90 percent of memory is used... allow some margin to avoid swapping
        return value
        

##############################################
    def _get_host_resources(self):
        fid = open('/proc/cpuinfo')
        cpu_count = 0
        for line in fid:
            if 'processor' in line:
                cpu_count = cpu_count + 1
        fid.close()
                
        fid = open('/proc/meminfo')
        line = fid.readline()
        linesplit = line.split()
        mem_kb_str = linesplit[1]
        mem_gb = int(int(mem_kb_str)/1024/1024)
        
        return (cpu_count, mem_gb)
        

    def _execute_str(self,cmd_str):

        p = subprocess.Popen(cmd_str,shell=True,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        (stdout,stderr) = p.communicate()
        return_code = p.returncode
        err = return_code!=0
        stdout_str = str(stdout)
        stderr_str = str(stderr)

        return (err,stdout_str,stderr_str)
    
    def launchJob(self,jobId,pipelineName, cmdStr,resources, job_outdir):
        if jobId in self._pipelineName_by_jobId:
            raise Exception ('jobId already launched: ' + jobId)
        
        niceCmdStr = 'nice -n 10 ' + cmdStr
        
        self._pipelineName_by_jobId[jobId] = pipelineName
        self._cmdStr_by_jobId[jobId] = niceCmdStr
        self._resources_by_jobId[jobId] = resources
        self._job_outdir_by_jobId[jobId] = job_outdir
        
        self._filehandles_by_jobId[jobId] = []

        self._localStatus_by_jobId[jobId] = '*UNLAUNCHED'
        print ('Launch job: ' + jobId + '\n',
               '   cmdStr: ' + cmdStr + '\n',
               '   pipelineName: ' + pipelineName + '\n',
               '   resources: ' + str(resources) + '\n',
               '   job_outdir: ' + job_outdir + '\n'
               )

    def getJobStatus(self,jobId):
        if jobId not in self._localStatus_by_jobId:
            return 'UNREGISTERED-LOCENG'
        
        localStatus = self._localStatus_by_jobId[jobId]
        
        # *Unlaunched, etc are my own status codes.
        if localStatus in ['*UNLAUNCHED']:
            status = "QUEUED"
        elif localStatus in ['*LAUNCHED']:
            status = "RUNNING"
        elif localStatus in ['EXIT', '*KILLED']:
            status = "FAIL"
        elif localStatus in ['DONE']:
            status = "PASS"
        elif localStatus in []:
            status = "ERROR"
        else:
            raise Exception('Unexpected lsfStatus code: ' + lsfStatus)
                            
        return status

    def killJob(self,jobId):
        print ('Kill job: ' + jobId)
        if jobId not in self._running_processes_by_jobId:
            return
            #raise Exception('unregistered jobId: '+jobId)
        self._localStatus_by_jobId[jobId] = '*KILLED'
        p = self._running_processes_by_jobId[jobId]
        try:
            p.kill()
        except:
            print ('failed to kill ' + jobId)
            
        for fid in self._filehandles_by_jobId[jobId]:
            fid.close()
        self._filehandles_by_jobId[jobId]=[]
        del self._running_processes_by_jobId[jobId]

    def update(self,jobPriorityList):
        # Launch as many new jobs as will fit, in strict priority order
        just_launched = []
        for jobId in jobPriorityList:
            if jobId in self._running_processes_by_jobId:
                # already running
                continue
            
            if jobId not in self._pipelineName_by_jobId:
                # not yet launched
                continue
            
            job_resources = self._resources_by_jobId[jobId]
            job_maxMem = float(job_resources['maxmem'])
            
            if len(self._running_processes_by_jobId)+1 > self._max_jobs():
                # hit the max job cap
                break
            current_mem = 0
            for runningJobId in self._running_processes_by_jobId:
                running_jobResources = self._resources_by_jobId[runningJobId]
                running_jobMaxMem = running_jobResources['maxmem']
                current_mem += float(running_jobMaxMem)
            if current_mem + job_maxMem > self._max_mem():
                # hit the max mem cap
                break
            
            # Launch a new job
            stdout_path = os.path.join(self._job_outdir_by_jobId[jobId], 'pipette.wrapper.stdout.txt')
            stderr_path = os.path.join(self._job_outdir_by_jobId[jobId], 'pipette.wrapper.stderr.txt')
            time.sleep(1)
            stdout_fid = open(stdout_path, 'w')
            stderr_fid = open(stderr_path, 'w')
            p = subprocess.Popen(self._cmdStr_by_jobId[jobId], bufsize=-1, shell=True, stdout = stdout_fid,
                                 stderr = stderr_fid, cwd = self._job_outdir_by_jobId[jobId])
            
            # Register the new job in the data structures
            self._running_processes_by_jobId[jobId] = p
            self._filehandles_by_jobId[jobId] = [stdout_fid,stderr_fid]
            self._localStatus_by_jobId[jobId] = '*LAUNCHED'
            print ('Launched ' + jobId)
            
            just_launched.append(jobId)
        
        
        
        # Update the status of already launched jobs
        running_jobs = list(self._running_processes_by_jobId.keys())
        for jobId in running_jobs:
            if jobId in just_launched:
                # give the status some time to update...
                continue
            p = self._running_processes_by_jobId[jobId]
            status = p.poll()
            if status is None:
                # still running
                continue
            elif status == 0:
                self._localStatus_by_jobId[jobId] = 'DONE'
                for fid in self._filehandles_by_jobId[jobId]:
                    fid.close()
                self._filehandles_by_jobId[jobId]=[]
                del self._running_processes_by_jobId[jobId]
                print ('Job passed: '+jobId)
            elif status > 0:
                self._localStatus_by_jobId[jobId] = 'EXIT'
                for fid in self._filehandles_by_jobId[jobId]:
                    fid.close()
                self._filehandles_by_jobId[jobId] = []
                del self._running_processes_by_jobId[jobId]
                print ('Job failed: '+jobId)
            else:
                self._localStatus_by_jobId[jobId] = '*KILL'
                for fid in self._filehandles_by_jobId[jobId]:
                    fid.close()
                self._filehandles_by_jobId[jobId] = []
                del self._running_processes_by_jobId[jobId]
                print ('Job killed: ' + jobId)
                    
    def getEngineJobId(self,jobId):
        if jobId not in self._running_processes_by_jobId:
            engineJobId = 'NOID-LOCAL'
        else:
            p = self._running_processes_by_jobId[jobId]
            engineJobId = p.pid
        return engineJobId
        
    def purgeJob(self,jobId):
        del self._pipelineName_by_jobId[jobId]
        del self._cmdStr_by_jobId[jobId]
        del self._resources_by_jobId[jobId]
        del self._job_outdir_by_jobId[jobId]

        del self._localStatus_by_jobId[jobId]
        del self._filehandles_by_jobId[jobId]
        
        print ('Job purged from engine: ' + jobId)
        
    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        # State that will be stored is as if none of the running jobs had been launched yet.
        #for jobId in state['_running_processes_by_jobId']:
            #state['_localStatus_by_jobId'][jobId] = '*UNLAUNCHED'
        state['_running_processes_by_jobId'] = {}
        state['_filehandles_by_jobId'] = {}
        return state


        
    def __setstate__(self,state):
        # This code is run when an instance is created by unpickling, and not when it is initially created.
        self.__dict__.update(state)
        # All running processes were detached upon restoration, and are uncontactable.

        
if __name__ == '__main__':
    pass
