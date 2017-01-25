import string
import random
import datetime
import time
import os
import sys
import platform
import getpass

class PipetteClient:
    def __init__(self, pipelineOutDir,
                 pipelineName='NamelessPipeline',
                 defaultCaching='False',
                 defaultCleanUpJobFilesOnFail='False',
                 defaultCleanUpPipelineJobsOnFail='True',
                 defaultExecutionEngine='mockPrint',
                 pipelinePriority=50,
                 injectionMap={},
                 communicationDirBase=None
                 ):
        
        # Set the communication dir in a platform specific manner.  
        # Must be modified in a new environment.
        
        if communicationDirBase == None:
            try:
                username = getpass.getuser()
            except KeyError:
                username = 'unknown'
            platform_name = platform.system()
            if platform_name == 'Windows':
                communicationDirBase = os.path.join('u:\\','pipette',username)
            elif platform_name == 'Linux':
                home = os.environ['HOME']
                communicationDirBase = os.path.join(home,'pipette_queue')
            else:
                raise Exception('unknown platform type')

        
        self._communication_dir_base = communicationDirBase
        self._launch_subdir = 'launch'
        self._status_subdir = 'status'
        
        
        if 'ipette' not in pipelineOutDir:
            raise Exception('"Pipette" or "pipette" must be somewhere in pipelineOutDir, for safety against rmtree')
        
        self._outfile_set = set()
        
        self._last_timestamp=''
        self._token_counter = 0
        
        pipelineTimestamp_val = self._get_timestamp()
        token = self._get_unique_token(pipelineTimestamp_val)
        pipelineId_val = token + '.' + pipelineName
        
        self._pipelineOutDir = pipelineOutDir
        self._pipelineName = pipelineName
        self._pipelineTimestamp = pipelineTimestamp_val
        self._pipelineId = pipelineId_val
        self._defaultCaching = defaultCaching
        self._defaultCleanUpJobFilesOnFail = defaultCleanUpJobFilesOnFail
        self._defaultCleanUpPipelineJobsOnFail = defaultCleanUpPipelineJobsOnFail
        self._defaultExecutionEngine = defaultExecutionEngine
        self._pipelinePriority = pipelinePriority
        self._injectionMap = injectionMap
        
        self._dispense_buffer = []
        self._launched = False

    def _substitute_macros(self,in_str,module_outdir):
        out_str = in_str
        if module_outdir != None:
            out_str = out_str.replace('$MODULEOUTDIR',module_outdir)
        out_str = out_str.replace('$PIPELINEOUTDIR',self._pipelineOutDir)
        for key in self._injectionMap:
            out_str = out_str.replace('$'+key,self._injectionMap[key])
        return out_str
   
    
    def _get_unique_token(self,timestamp_arg=None):
        #create unique string that sorts in order of creation
        
        
        #get a timestamp string, ordered years to seconds
        if timestamp_arg==None:
            ts = self._get_timestamp()
        else:
            ts = timestamp_arg
        
        # set token counter to make timestamp unique within this instance of the pipette client.
        if ts == self._last_timestamp:
            self._token_counter = self._token_counter+1
        else:
            self._token_counter = 0
        self._last_timestamp = ts
        token_counter_str = str(self._token_counter).zfill(3)
        
        uniq_hash = self._get_uniq_hash()
        
        token = ts + '.' + token_counter_str + '.' + uniq_hash
       
        return token
    def _get_uniq_hash(self):
        #set random 6 character string, for easier human readability of the id's.
        uniq_hash = ''
        for i in range(6):
            uniq_hash = uniq_hash + random.choice(string.ascii_letters)
        return uniq_hash

    def _get_timestamp(self):
        t=datetime.datetime.now()
        timestamp = str(t.year) + '_' + str(t.month).zfill(2) + '_' + str(t.day).zfill(2) + '__' + \
                  str(t.hour).zfill(2) + '_' + str(t.minute).zfill(2) + '_' + str(t.second).zfill(2)
        return timestamp
        
    

    def _get_timestamp_delta(self,ts_begin,ts_end):        
        begin_secs = timestamp_to_seconds(ts_begin)
        end_secs = timestamp_to_seconds(ts_end)
        duration = end_secs - begin_secs
        return duration
    
    def _timestamp_to_seconds(self,timestamp):
        (year,month,day,junk,hour,minute,second)= timestamp.split('_')
        (year,month,day,junk,hour,minute,second) = (
            int(year),int(month),int(day),junk,int(hour),int(minute),int(second))
        
        dt = datetime.datetime(year,month,day,hour,minute,second)
        days = dt.toordinal()
        days = days - 733000 # keep the seconds to a manageable size...
        secs = days*24*60*60 + hour*60*60 + minute*60 + second
        return secs
        
    
    
    def dispense(self, moduleSubDir, cmdStr, resources, jobName= "NamelessJob", inputFiles=[], 
                 filesToBeOutput=[], filesToBeDeleted=[],
                 caching='PipelineDefault',cleanUpJobFilesOnFail='PipelineDefault',
                 cleanUpPipelineJobsOnFail='PipelineDefault',
                 executionEngine='PipelineDefault'):
        
        
        if self._launched == True:
            raise Exception ('pipeline already launched, so a new pipeline must be instatiated to continue')
       # Create unique jobId.
        
        token = self._get_unique_token()
        moduleSubDir_thunked = moduleSubDir.replace(os.path.sep,'_')
        
        jobId = token + '.' + self._pipelineName + '.' + jobName + '.' + moduleSubDir_thunked 
        module_outdir = os.path.join(self._pipelineOutDir,moduleSubDir)
        
        
        # Expand macros in all args except in jobName and outSubDir.
        
        inputFiles_sub = []
        for fn in inputFiles:
            fn_substituted = self._substitute_macros(fn,module_outdir)
            inputFiles_sub.append(fn_substituted)

        filesToBeOutput_sub = []
        for fn in filesToBeOutput:
            fn_substituted = self._substitute_macros(fn,module_outdir)
            filesToBeOutput_sub.append(fn_substituted)
            
        filesToBeDeleted_sub = []
        for fn in filesToBeDeleted:
            fn_substituted = self._substitute_macros(fn,module_outdir)
            filesToBeDeleted_sub.append(fn_substituted)
            
        resources_sub={}
        for key in resources:
            key_sub = self._substitute_macros(key,module_outdir)
            value_sub = self._substitute_macros(str(resources[key]),module_outdir)
            resources_sub[key_sub]=value_sub
            
        # Do NOT replace JOBOUTDIR in cmdStr, that must be done later
        cmdStr_sub = self._substitute_macros(cmdStr,None)

        caching_sub = self._substitute_macros(caching,module_outdir)
        cleanUpJobFilesOnFail_sub = self._substitute_macros(cleanUpJobFilesOnFail,module_outdir)
        cleanUpPipelineJobsOnFail_sub = self._substitute_macros(cleanUpPipelineJobsOnFail,module_outdir)
        executionEngine_sub = self._substitute_macros(executionEngine,module_outdir)
        
        
        # Substitute pipeline default values
        
        if caching_sub == 'PipelineDefault':
            caching_sub = self._defaultCaching
            
        if cleanUpJobFilesOnFail_sub == 'PipelineDefault':
            cleanUpJobFilesOnFail_sub = self._defaultCleanUpJobFilesOnFail
            
        if cleanUpPipelineJobsOnFail_sub == 'PipelineDefault':
            cleanUpPipelineJobsOnFail_sub = self._defaultCleanUpPipelineJobsOnFail
            
        if executionEngine_sub == 'PipelineDefault':
            executionEngine_sub = self._defaultExecutionEngine
        
            
        # validate scalar arguments

        if caching_sub not in ['True','False']:
            raise Exception('caching must resolve to True or False.  raw: '+caching+' substituted:'+caching_sub)
        
        if cleanUpJobFilesOnFail_sub not in ['True','False']:
            raise Exception('cleanUpJobFilesOnFail must resolve to True or False.  raw: '+cleanUpJobFilesOnFail+' substituted:'+cleanUpJobFilesOnFail_sub)

        
        if cleanUpPipelineJobsOnFail_sub not in ['True','False']:
            raise Exception('cleanUpPipelineJobsOnFail must resolve to True or False.  raw: '+cleanUpPipelineJobsOnFail+' substituted:'+cleanUpPipelineJobsOnFail_sub)

        if 'maxmem' not in resources_sub or 'maxtime' not in resources_sub:
            raise Exception('maxmem and maxtime resources must be specified')
        
        

        # validate input files.
        # For ones which are expected earlier in pipeline, verify than some previous module is making it.
        # For ones which come from outside the pipeline, verify that they already exist.
        pipeline_infiles = []
        for infile in inputFiles_sub:
            if infile.startswith(self._pipelineOutDir):
                if infile in self._outfile_set:
                    pipeline_infiles.append(infile)
                    #ok - file has already been listed as output file earlier in pipeline
                else:                    
                    raise Exception('Invalid input file - expected to be created by pipeline, but no previous module has listed it as an output file\n' + \
                                    infile + '\n'+
                                    'files present: ' + str(self._outfile_set) + '\n')
            else:
                if os.path.exists(infile):
                    pass
                    #ok - file outside of pipeline, and already exists 
                else:
                    raise Exception('Invalid input file - file outside of pipeline, and does not yet exist\n' +\
                                    infile + '\n')

        # record the deleted files, for subsequent jobs
        for fn in filesToBeDeleted_sub:
            self._outfile_set.remove(fn)
            
        # record the output files, for subsequent jobs
        for fn in filesToBeOutput_sub:
            self._outfile_set.add(fn)


        self._dispense_buffer.append("formatVersion-\t3\n")

        self._dispense_buffer.append('moduleSubDirName-\t'+moduleSubDir_thunked+'\n')
        self._dispense_buffer.append('jobId-\t' + jobId + '\n')
        self._dispense_buffer.append('jobName-\t' + jobName + '\n')
        self._dispense_buffer.append('pipelineId-\t' + self._pipelineId + '\n')
        self._dispense_buffer.append('pipelineName-\t' + self._pipelineName + '\n')
        self._dispense_buffer.append('pipelineTimestamp-\t' + self._pipelineTimestamp + '\n')
        self._dispense_buffer.append('pipelinePriority-\t' + str(self._pipelinePriority) + '\n')

        self._dispense_buffer.append('cmdStr-\t' + cmdStr_sub + '\n')

        # Guaranteed to be at least two keys in the resources dict
        res_str = ''
        for key in resources_sub:
            res_str = res_str + '\t' + key + '\t' + resources_sub[key]
        self._dispense_buffer.append('resources:'+res_str+'\n')

        self._dispense_buffer.append('pipelineOutDir-\t' + self._pipelineOutDir + '\n')
        self._dispense_buffer.append('moduleOutDir-\t' + module_outdir + '\n')

        infiles_tabs = '\t'.join(pipeline_infiles) + '\n'
        self._dispense_buffer.append('inFiles=\t'+infiles_tabs)

        outfiles_tabs = '\t'.join(filesToBeOutput_sub) + '\n'
        self._dispense_buffer.append('outFiles=\t'+outfiles_tabs)

        deletefiles_tabs = '\t'.join(filesToBeDeleted_sub) + '\n'
        self._dispense_buffer.append('deleteFiles=\t' + deletefiles_tabs)

        self._dispense_buffer.append('caching-\t'+caching_sub+'\n')
        self._dispense_buffer.append('cleanUpJobFilesOnFail-\t'+cleanUpJobFilesOnFail_sub+'\n')
        self._dispense_buffer.append('cleanUpPipelineJobsOnFail-\t'+cleanUpPipelineJobsOnFail_sub+'\n')
        self._dispense_buffer.append('executionEngine-\t'+executionEngine_sub+'\n')

        self._dispense_buffer.append('EOM-\t\n')
        
        return filesToBeOutput_sub
        
    def go(self):
        self._dispense_buffer.append("EOF-\t\n")
  
        # write dispense file for pipetteServer
        dispense_dir = os.path.join(self._communication_dir_base,self._launch_subdir)
        if not os.path.exists(dispense_dir):
            try:
                os.makedirs(dispense_dir)
            except:
                pass
        fn_base = self._pipelineId + ".launch.txt"
        fn = os.path.join(dispense_dir,fn_base)
        fid = open(fn,"wt")
        fid.writelines(self._dispense_buffer)
        fid.close()
        
        self._launched = True
            
    def wait(self,timeout=None):
        polling_interval = 60 
        preliminary_passes = 3
        return_status = self.status()
        # Allow some time for pipeline to initially launch
        if return_status == 'Error':
            for i in range(preliminary_passes):
                time.sleep(polling_interval)            
                return_status = self.status()
                if return_status != 'Error':
                    break
        # Wait until pipeline finishes or until time is up
        while return_status == 'InProgress':
            time.sleep(polling_interval)            
            if timeout!=None:
                current_timestamp = self._get_timestamp()
                pipeline_duration = self._get_timestamp_delta(self._pipelineTimestamp,current_timestamp)
                if pipeline_duration > timeout:
                    break
            return_status = self.status()

        return return_status
    
    def status(self):
        status_filename = os.path.join(self._communication_dir_base,self._status_subdir,'pipeline_only.status.txt')
        fid = open(status_filename,'rt')
        return_status = 'Error'
        while line in fid:
            line_list = split(line)
            pipelineStatus = line_list[0]
            pipelineId = line_list[1]
            if pipelineId == self._pipelineId:
                return_status = pipelineStatus
                break
        fid.close()
            
        return return_status
 