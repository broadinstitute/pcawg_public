#!/usr/bin/env python3

import os
import csv
import sys
import pickle
import shutil
import getpass
import datetime
import platform
import collections
import time
import cProfile
import threading
import queue
import atexit
import optparse
import LocalEngine

class Jobs:
    def __init__(self, commdir):
        # This code is run when starting a new instance, and not when the instance is unpickled.
        self._file_ext = '.launch.txt'
        self._job_by_jobId = {}
        self._commdir = commdir
        launch_subdir = 'launch'
        archive_subdir = 'archive'
        self._launch_dir = os.path.join(self._commdir, launch_subdir)
        self._archive_dir = os.path.join(self._commdir, archive_subdir)
        try:
            os.makedirs(self._launch_dir,exist_ok=True)
        except:
            raise Exception('Failed to create launchdir; communicationDirBase may be read-only')

        os.makedirs(self._archive_dir,exist_ok=True)

        existing_launchfiles = os.listdir(self._launch_dir)
        if existing_launchfiles:
            print ('Starting with ' + str(len(existing_launchfiles)) + ' launched but unprocessed pipelines.')
        else:
            print ('Starting with 0 pipelines')
        existing_archivefiles = os.listdir(self._archive_dir)
        if len(existing_archivefiles) > 0:
            raise Exception ('starting with ' + str(len(existing_archivefiles)) + ' in-process jobs... either these should be purged from:' + 
                             self._archive_dir + ' or things shoud be restarted from the persisted state')
        
    def load_new_job(self, job_filename):
        jobIds = []
        job_filepath = os.path.join(self._launch_dir, job_filename)
        infid = open(job_filepath, 'rt')
        file_lines = infid.readlines()
        infid.close()
        if not file_lines or not file_lines[-1].startswith('EOF'):
            return jobIds
        
        jobdict = {}
        jobdict['dispenseName'] = job_filename
        for linestr in file_lines:
            line = linestr[:-1].split('\t')
            
            key = line[0][:-1]
            type = line[0][-1]
            if type == '-':
                # scalar
                value = line[1]
            elif type == '=':
                # list
                if line[1] == '':
                    value = []
                else:
                    value = line[1:]
            elif type == ':':
                # dict
                keys = line[1::2]
                values = line[2::2]
                value = dict(zip(keys,values))
            #print (key,str(value))
            if key == 'formatVersion':
                if value != '3':
                    # TODO perhaps should just reject the dispense file?
                    raise Exception('unknown format for dispense file: '+value)
            elif key == 'EOF':
                break
            elif key == 'EOM':
                jobId = jobdict['jobId']
                jobIds.append(jobId)
                if jobId in self._job_by_jobId:
                    raise Exception('job already loaded: '+jobId)
                self._job_by_jobId[jobId] = jobdict
                #reset jobdict for the next job
                jobdict = {}
                jobdict['dispenseName'] = job_filename
            elif key == 'DummyFile':
                raise Exception('attempting to load dummy dispense file, this should not happen')
            else:
                jobdict[key] = value
        else:
            # TODO should this be a warning? or silently ignored?
            raise Exception('incomplete dispense file: ' + job_filepath)        
        
        # move dispense file out of the way
        archive_job_path = os.path.join(self._archive_dir, job_filename)
        
        # file should not exist already in archive - will cause error in the next statement if it does.
        # presumably this would happen by someone messing around with the dispense files
        os.rename(job_filepath,archive_job_path)
        
        return jobIds
    
    def get_new_jobfiles(self):
        raw_jobfiles = os.listdir(self._launch_dir)
        # Filter out files with the wrong extension.
        # We don't want to have a method in this class to load them all right away, 
        #  just in case there are so many they delay other things too much.
        jobfiles = [fn for fn in raw_jobfiles if fn.endswith(self._file_ext)]
        return jobfiles

    def get_jobIds(self):
        jobIds = list(self._job_by_jobId.keys())

        return jobIds
    def get_job(self,jobId):
        # Returning just a copy, for safety.
        job = self._job_by_jobId[jobId].copy()
        return job

    def purge_job(self,jobId):
        dispense_filename = self._job_by_jobId[jobId]['dispenseName']
        dispense_archive_filepath = os.path.join(self._archive_dir, dispense_filename)
        if os.path.exists(dispense_archive_filepath):
            os.remove(dispense_archive_filepath)
        del self._job_by_jobId[jobId]
        
    def __setstate__(self,state):
        # This code is run when an instance is created by unpickling, and not when it is initially created.
        self.__dict__.update(state)
        # Make the disk files consistent with the internal variable state.
        file_ext_len = len(self._file_ext)
        launch_files = os.listdir(self._launch_dir)
        archive_files = os.listdir(self._archive_dir)
        launch_file_jobIds = set()
        for fn in launch_files:
            # error if we find a file here that we think we already processed
            if not fn.endswith(self._file_ext):
                continue
            file_jobId = fn[:-file_ext_len]
            launch_file_jobIds.add(file_jobId)
            if file_jobId in self._job_by_jobId:
                raise Exception('Found a job in the launch directory that we should have already processed')
            else:
                # This must be a new job... no problem.
                pass
            
        archive_file_jobIds = set()
        for fn in archive_files:
            # If we find a file here that it looks like we have not processed, move it back to launched so it can be processed again.
            if not fn.endswith(self._file_ext):
                continue
            file_jobId = fn[:-file_ext_len]
            archive_file_jobIds.add(file_jobId)
            if file_jobId not in self._job_by_jobId:
                archive_filepath = os.path.join(self._archive_dir, fn)
                launch_filepath = os.path.join(self._launch_dir, fn)
                os.rename(archive_filepath, launch_filepath)
                
        for jobId in self._job_by_jobId:
            # if we find a job that we think we are still processing, but no dispense file in the archive directory,
            # then create a fake job file there.  For now don't bother filling it in with the real fields, but we do
            # have everything in memory.
            if jobId not in archive_file_jobIds:
                #print('DummyFile-')
                archive_filename = jobId + self._file_ext
                archive_filepath = os.path.join(self._archive_dir, archive_filename)
                fid = open(archive_filepath, 'wt')
                fid.write('DummyFile-\t\n')
                fid.close()
                

class JobDirectoryManager:
    def __init__(self, do_retry):
        self._moduleDirectory_by_jobId = {}
        self._jobDirectory_by_jobId = {}
        self._cleanupOnFail_by_jobId = {}
        self._status_filename = 'pipette.module_retrydir_status.txt'
        self._status_by_jobId = {}
        self._do_retry = do_retry
        pass

    def _jobId_to_jobSubdir(self, jobId):
        return 'pipette.' + jobId

    def get_job_directory(self, jobId):
        return self._jobDirectory_by_jobId[jobId]

    def get_directory_pass_status(self, jobId):
        dir_status = self._get_module_directory_status(jobId)
        if dir_status == 'Pass':
            dir_pass_status = 'PASS'
        else:
            dir_pass_status = 'NOTPASS'
        return dir_pass_status
    
    def newJob(self, jobId, moduleOutDir, cleanUpJobFilesOnFail):
        self._moduleDirectory_by_jobId[jobId] = moduleOutDir
        self._cleanupOnFail_by_jobId[jobId] = cleanUpJobFilesOnFail
        if self._do_retry:
            job_dir = os.path.join(moduleOutDir, self._jobId_to_jobSubdir(jobId))
        else:
            job_dir = moduleOutDir
        self._jobDirectory_by_jobId[jobId] = job_dir
        self._status_by_jobId[jobId] = 'Empty'

    def create_fresh_job_directory(self, jobId):
        job_dir = self._jobDirectory_by_jobId[jobId]
        module_outdir = self._moduleDirectory_by_jobId[jobId]

        purge_module_dir = 'Maybe'
        
        if not os.path.exists(module_outdir):
            purge_module_dir = 'No'
           
        if purge_module_dir == 'Maybe':
            status = self._get_module_directory_status(jobId)
            if status in ['Pass', 'AboutToPass']:
                purge_module_dir = 'Yes'
        
        if purge_module_dir == 'Maybe' and os.path.exists(module_outdir):
            # Don't hit the file system unless necessary...
            module_files = os.listdir(module_outdir)
            for fn in module_files:
                if not fn.startswith('pipette.'):
                    purge_module_dir = 'Yes'
                    break
                
        if purge_module_dir == 'Maybe':
            purge_module_dir = 'No'
        
        if purge_module_dir == 'Yes':
            # Wipe the module dir, unless it has only benign contents
            shutil.rmtree(module_outdir)
                
        if os.path.exists(job_dir):
            # Always wipe clean the job dir
            shutil.rmtree(job_dir)
            
        # Make the job_dir, and the module_dir as well if needed.   
        os.makedirs(job_dir, exist_ok=True)

        # removed sleep

        # Update the status file at the end, to indicate that all changes have been made.
        self._set_module_directory_status(jobId,jobId)

    def cleanup_for_passing(self,jobId):
        # Check for tampering with module directory contents.
        initial_status = self._get_module_directory_status(jobId)
        if initial_status == 'Pass':
            return 'Pass'
        if initial_status != jobId:
            return 'Abort'
        module_dir = self._moduleDirectory_by_jobId[jobId]
        if self._do_retry:
            module_files = os.listdir(module_dir)
            for fn in module_files:
                if not fn.startswith('pipette.'):
                    return 'Abort'

        # Move forward with passing protocol...
        self._set_module_directory_status(jobId, 'AboutToPass')
        
        job_dir = self._jobDirectory_by_jobId[jobId]

        if self._do_retry:
            # First move the passing directory up one level
            try:
                job_files = os.listdir(job_dir)
                for fn in job_files:
                    old_path = os.path.join(job_dir, fn)
                    new_path = os.path.join(module_dir, fn)
                    os.rename(old_path, new_path)
                os.rmdir(job_dir)
            except:
                # If failing... wait a sec and try again, perhaps some filehandles did not close yet.
                try:
                    time.sleep(1)
                    job_files = os.listdir(job_dir)
                    for fn in job_files:
                        old_path = os.path.join(job_dir, fn)
                        new_path = os.path.join(module_dir, fn)
                        os.rename(old_path, new_path)
                    os.rmdir(job_dir)
                except:
                    print ('failed to move ' + job_dir + ' to passing location')
            # Purge any failing job directories
            try:
                job_subdir = self._jobId_to_jobSubdir(jobId)
                module_files.remove(job_subdir)
                module_files.remove(self._status_filename)
                for fn in module_files:
                    f_path = os.path.join(module_dir, fn)
                    shutil.rmtree(f_path)
            except:
                print ('failed to remove failing job directories under ' + module_dir)

        final_status = self._get_module_directory_status(jobId)
        if final_status != 'AboutToPass':
            return 'Abort'
        self._set_module_directory_status(jobId, 'Pass')
        return 'Pass'

    def cleanup_for_failing(self, jobId):
        initial_status = self._get_module_directory_status(jobId)
        if initial_status == 'Fail':
            return 'Pass'
        if initial_status != jobId:
            return 'Abort'
            
        # Move forward with failing protocol...
        self._set_module_directory_status(jobId, 'AboutToFail')
        if self._cleanupOnFail_by_jobId[jobId] == 'False':
            pass
        elif self._cleanupOnFail_by_jobId[jobId] == 'True':
            if self._do_retry:
                try:
                    job_dir = self._jobDirectory_by_jobId[jobId]
                    if os.path.exists(job_dir):
                        shutil.rmtree(job_dir)
                except:
                    raise Exception('failed to remove failing job directory: ' + job_dir)
            else:
                #job dependent runtime error
                raise Exception('cleanupOnFail not supported when retries are disabled')
            
        else:
            raise Exception('Unexpected value for CleanupOnFail: ' + self._cleanupOnFail_by_jobId[jobId])
        
        final_status = self._get_module_directory_status(jobId)
        if final_status != 'AboutToFail':
            return 'Abort'
        self._set_module_directory_status(jobId,'Fail')
        return 'Pass'

    def cleanup_for_aborting(self, jobId):
        # Module directory does not even exist yet! No need to create it or record anything.
        if not os.path.exists(self._moduleDirectory_by_jobId[jobId]):
            return 'Pass'
        # Don't touch anything if the jobId does not match.
        initial_status = self._get_module_directory_status(jobId)
        if initial_status == 'Abort':
            return 'Pass'
        if initial_status != jobId:
            return 'Abort'
            
        # Move forward with aborting protocol...
        self._set_module_directory_status(jobId, 'AboutToAbort')
        if self._cleanupOnFail_by_jobId[jobId] == 'False':
            pass
        elif self._cleanupOnFail_by_jobId[jobId] == 'True':
            if self._do_retry:
                try:
                    job_dir = self._jobDirectory_by_jobId[jobId]
                    # Purge just the directory for this job.
                    if os.path.exists(job_dir):
                        shutil.rmtree(job_dir)
                except:
                    raise Exception('failed to remove failing job directory: ' + job_dir)
            else:
                raise Exception('cleanupOnFail not supported when retries are disabled')
        else:
            raise Exception('Unexpected value for CleanupOnFail: ' + self._cleanupOnFail_by_jobId[jobId])
        
        final_status = self._get_module_directory_status(jobId)
        if final_status != 'AboutToAbort':
            return 'Abort'
        self._set_module_directory_status(jobId, 'Abort')
        return 'Pass'

    def purge_job(self, jobId):
        # Leave output directory and its status alone
        del self._moduleDirectory_by_jobId[jobId]
        del self._jobDirectory_by_jobId[jobId]
        del self._cleanupOnFail_by_jobId[jobId]
        del self._status_by_jobId[jobId]
                        
    def _set_module_directory_status(self, jobId, status):
        module_dir = self._moduleDirectory_by_jobId[jobId]
        filepath = os.path.join(module_dir, self._status_filename)
        fid = open(filepath, 'wt')
        fid.write(status + '\n')
        fid.close()
        
        self._status_by_jobId[jobId]=status
        
    def _get_module_directory_status(self, jobId):
        if jobId not in self._moduleDirectory_by_jobId:
            return 'UNREGISTERED-DIR'
        module_dir = self._moduleDirectory_by_jobId[jobId]
        filepath = os.path.join(module_dir, self._status_filename)
        if not os.path.exists(filepath):
            return 'Empty'
        fid = open(filepath, 'rt')
        msg = fid.readline()
        fid.close()
        msg = msg.strip()
        return msg
    
    def __setstate__(self, state):
        # This code is run when an instance is created by unpickling, and not when it is initially created.
        self.__dict__.update(state)
        # roll back jobs that were sent into some state? make these transitions tolerant of being called twice in a row?
        # how do jobs get purged that finished?

        for jobId in self._status_by_jobId:
            directory_status = self._get_module_directory_status(jobId)
            if directory_status == self._status_by_jobId[jobId]:
                continue
            if directory_status in ['AboutToFail','AboutToAbort']:
                # just try again
                self._set_module_directory_status(jobId, jobId)
            elif directory_status in ['Pass','Fail', jobId]:
                pass
            elif directory_status in ['AboutToPass']:
                # move all the stuff back into the jobdir and try again
                jobdir = self._jobDirectory_by_jobId[jobId]
                moddir = self._moduleDirectory_by_jobId[jobId]
                if not os.path.exists(jobdir):
                    os.mkdir(jobdir)
                module_files = os.listdir(jobdir)
                for fn in module_files:
                    if fn.startswith('pipette.'):
                        continue
                    old_path = os.path.join(moddir, fn)
                    new_path = os.path.join(jobdir, fn)
                    os.rename(old_path, new_path)
                # _after_ the files are moved, update the status
                self._set_module_directory_status(jobId, jobId)
            else:
                raise Exception('Module directory left in an unrepairable state: ' + \
                                self._moduleDirectory_by_jobId[jobId])


class ExecutionEngines:
    def __init__(self,engineDict, scriptDir, do_retry):
        self._engineDict_by_engineName = engineDict
        self._scriptDir = scriptDir
        self._engineName_by_jobId = {}
        self._cmdStr_by_jobId = {}
        self._resources_by_jobId = {}
        self._job_outdir_by_jobId = {}
        self._pipelineName_by_jobId = {}
        self._do_retry = do_retry


    def newJob(self, jobId, pipelineName, engineName, cmdStr, resources, job_outdir):
        if engineName not in self._engineDict_by_engineName:
            print ('unknown engine name: ' + engineName)
            # Throw exception? This could be a user-input error.
            # Perhaps better to abort the job.
            raise Exception ('unknown engine name: ' + engineName)
        self._engineName_by_jobId[jobId] = engineName
        self._cmdStr_by_jobId[jobId] = cmdStr
        self._resources_by_jobId[jobId] = resources
        self._job_outdir_by_jobId[jobId] = job_outdir
        self._pipelineName_by_jobId[jobId] = pipelineName

    def _tweakCmdStr(self, cmdStr, job_outdir, maxmem, maxtime):
        wrapper_script = os.path.join(self._scriptDir, 'pipetteJobWrapper.py')
        #cmdStrOut = cmdStr
        # single quote needed on Linux, to avoid having Env var expanded
        # double quote needed on Windows, as that is the only type recognized
        plat = platform.system()
        if plat == 'Windows':
            quote = '"'
        elif plat == 'Linux':
            quote = "'"
        else:
            raise Exception ('unsupported platform type: ' + plat)
        
        cmdStrOut = 'python "' + wrapper_script + '" "' + job_outdir + '" ' + \
                    maxmem + ' ' + maxtime + ' ' + str(self._do_retry) + ' ' + \
                    quote + cmdStr + quote
        return cmdStrOut
    
    def launchJob(self, jobId):
        pipelineName = self._pipelineName_by_jobId[jobId]
        engineName = self._engineName_by_jobId[jobId]
        cmdStr = self._cmdStr_by_jobId[jobId]
        resources = self._resources_by_jobId[jobId]
        job_outdir = self._job_outdir_by_jobId[jobId]

        maxmem = resources['maxmem']
        maxtime = resources['maxtime']
        
        cmdStr_wrapped = self._tweakCmdStr(cmdStr, job_outdir, maxmem, maxtime)

        engine = self._engineDict_by_engineName[engineName]
        engine.launchJob(jobId, pipelineName, cmdStr_wrapped, resources, job_outdir)
        
    def getJobStatus(self, jobId):
        if jobId not in self._engineName_by_jobId:
            return 'UNREGISTERED-ENG'
        
        engineName = self._engineName_by_jobId[jobId]
        engine = self._engineDict_by_engineName[engineName]
        status = engine.getJobStatus(jobId)
        return status
    
    def getEngineJobId(self, jobId):
        if jobId not in self._engineName_by_jobId:
            return 'NOID-ENG'       
        engineName = self._engineName_by_jobId[jobId]
        engine = self._engineDict_by_engineName[engineName]
        engineJobId = engine.getEngineJobId(jobId)
        return engineJobId
    
    def killJob(self,jobId):
        eng_name = self._engineName_by_jobId[jobId]
        engine = self._engineDict_by_engineName[eng_name]
        engine.killJob(jobId)
        
    def update(self,jobPriorityList):
        active_engines = set(self._engineName_by_jobId.values())
        for eng_name in active_engines:
            engine = self._engineDict_by_engineName[eng_name]
            engine.update(jobPriorityList)
            
    def purgeJob(self, jobId):
        del self._engineName_by_jobId[jobId]
        del self._cmdStr_by_jobId[jobId]
        del self._resources_by_jobId[jobId]
        del self._job_outdir_by_jobId[jobId]
        del self._pipelineName_by_jobId[jobId]
            

class State:
    def __init__(self):
        self._pipelineId_by_jobId = {}
        self._jobIds_by_pipelineId = {}
        self._pendingJobIds_by_pipelineId = {}
        self._outfiles_by_jobId = {}
        self._jobId_by_outfile = {}
        self._pendingParentJobs_by_jobId = {}
        self._pendingChildJobs_by_jobId = {}
        self._state_by_jobId = {}
        self._state_by_pipelineId = {}
        self._cleanUpPipelineJobsOnFail_by_jobId = {}
        
    def newJob(self, jobId, pipelineId, inFiles, outFiles, cleanUpPipelineJobsOnFail):
        if jobId in self._pipelineId_by_jobId:
            raise Exception ('jobId already in use: ' + jobId)
        
        pipeline_state = self._state_by_pipelineId.get(pipelineId)

        if pipeline_state is None:
            # initialize the pipeline state
            self._state_by_pipelineId[pipelineId]='InProgress'
        elif pipeline_state != 'InProgress':
            # Job launched after pipeline completed... too late.
            # Also make sure the pipeline is marked fail.
            self._state_by_jobId[jobId] = 'Abort'
            self._state_by_pipelineId[pipelineId] = 'Fail'
            return        
        
        # map between jobId and pipelineId
        self._pipelineId_by_jobId[jobId] = pipelineId
        if pipelineId not in self._jobIds_by_pipelineId:
            self._jobIds_by_pipelineId[pipelineId] = set()
            self._pendingJobIds_by_pipelineId[pipelineId] = set()
        self._jobIds_by_pipelineId[pipelineId].add(jobId)
        self._pendingJobIds_by_pipelineId[pipelineId].add(jobId)
        
        # Find which parent jobs have outstanding files to deliver - or have failed.
        pending_parent_jobs = set()
        abort_job = False
        for inFile in inFiles:
            parentJob = self._jobId_by_outfile[inFile]
            parentJobState = self._state_by_jobId[parentJob]
            if parentJobState in ['ReadyToAbort', 'Abort', 'Fail']:
                abort_job = True
                pending_parent_jobs = []
                break
            elif parentJobState in ['ReadyToStart', 'Started', 'WaitingForDependencies']:
                pending_parent_jobs.add(parentJob)
            elif parentJobState in ['Pass']:
                pass
            else:
                raise Exception('unexpected parent job state: ' + parentJobState)
            
        # map between parent and child jobs, set the state. 
        self._pendingParentJobs_by_jobId[jobId] = set()
        self._pendingChildJobs_by_jobId[jobId] = set()
        if abort_job:
            self._state_by_jobId[jobId] = 'ReadyToAbort'
        elif not pending_parent_jobs:
            self._state_by_jobId[jobId] = 'ReadyToStart'
        else:
            self._state_by_jobId[jobId] = 'WaitingForDependencies'
            self._pendingParentJobs_by_jobId[jobId] = pending_parent_jobs
            for parentJob in pending_parent_jobs:
                self._pendingChildJobs_by_jobId[parentJob].add(jobId)
            
        # map between jobId and outFiles
        self._outfiles_by_jobId[jobId] = []
        for outFile in outFiles:
            if outFile in self._jobId_by_outfile:
                #TODO this exception needs to be handled more gracefully - possible user error.
                # should somehow abort the job or pipeline and notify the user of the problem.
                raise Exception ('outfile name not unique: ' + outFile)
            self._jobId_by_outfile[outFile] = jobId
        self._outfiles_by_jobId[jobId] = outFiles
        
        # record this input argument
        self._cleanUpPipelineJobsOnFail_by_jobId[jobId] = cleanUpPipelineJobsOnFail
 
    def passJob(self, jobId):
        if jobId not in self._pipelineId_by_jobId:
            raise Exception('Pass job called on a job that was never initialized')
        pipelineId = self._pipelineId_by_jobId[jobId]
        if jobId not in self._pendingJobIds_by_pipelineId[pipelineId]:
            raise Exception('Pass job called on a job which has already been terminated: ' + jobId)
        
        self._state_by_jobId[jobId] = 'Pass'
        self._pendingJobIds_by_pipelineId[pipelineId].remove(jobId)
        
        # Update the state of the child jobs
        child_jobs = self._pendingChildJobs_by_jobId[jobId]
        for child_job in child_jobs:
            self._pendingParentJobs_by_jobId[child_job].remove(jobId)
            if (not self._pendingParentJobs_by_jobId[child_job] and
                self._state_by_jobId[child_job] == 'WaitingForDependencies'):
                self._state_by_jobId[child_job] = 'ReadyToStart'

        if not self._pendingJobIds_by_pipelineId[pipelineId]:
            self._set_final_pipeline_state(pipelineId)
 
    def failJob(self, jobId):
        if jobId not in self._pipelineId_by_jobId:
            raise Exception('fail job called on a job that was never initialized')
        pipelineId = self._pipelineId_by_jobId[jobId]
        if jobId not in self._pendingJobIds_by_pipelineId[pipelineId]:
            raise Exception('fail job called on a job which has already been terminated: ' + jobId)
    
        self._state_by_jobId[jobId] = 'Fail'
        self._pendingJobIds_by_pipelineId[pipelineId].remove(jobId)
        
        if self._cleanUpPipelineJobsOnFail_by_jobId[jobId] == 'True':
            self._abort_pipeline(pipelineId)
        elif self._cleanUpPipelineJobsOnFail_by_jobId[jobId] == 'False':
            child_jobs = self._pendingChildJobs_by_jobId[jobId]
            for child_job in child_jobs:
                self._abort_jobDependents(child_job)
        else:
            raise Exception ('unexpected value for cleanUpPipelineJobsOnFail: ' +
                             self._cleanUpPipelineJobsOnFail_by_jobId[jobId])

        if not self._pendingJobIds_by_pipelineId[pipelineId]:
            self._set_final_pipeline_state(pipelineId)
        
    def abortJob(self, jobId):
        if jobId not in self._pipelineId_by_jobId:
            raise Exception('abort job called on a job that was never initialized')
        pipelineId = self._pipelineId_by_jobId[jobId]
        if jobId not in self._pendingJobIds_by_pipelineId[pipelineId]:
            raise Exception('abort job called on a job which has already been terminated: ' + jobId)

        if self._cleanUpPipelineJobsOnFail_by_jobId[jobId] == 'True':
            self._abort_pipeline(pipelineId)
        elif self._cleanUpPipelineJobsOnFail_by_jobId[jobId] == 'False':
            self._abort_jobDependents(jobId)
        else:
            raise Exception ('unexpected value for cleanUpPipelineJobsOnFail: ' +
                             self._cleanUpPipelineJobsOnFail_by_jobId[jobId])
            
    def _abort_pipeline(self, pipelineId):
        pipelineJobs = self._pendingJobIds_by_pipelineId[pipelineId]
        for jobId in pipelineJobs:
            self._abort_single_job(jobId)
        
    def _abort_jobDependents(self, jobId):
        jobs = self._pendingChildJobs_by_jobId[jobId]
        for job in jobs:
            # recursively abort
            self._abort_jobDependents(job)
        self._abort_single_job(jobId)
        
    def _abort_single_job(self, jobId):
        if self._state_by_jobId[jobId] in ['Started', 'ReadyToStart', 'WaitingForDependencies']:
            self._state_by_jobId[jobId]='ReadyToAbort'
        elif self._state_by_jobId[jobId] in ['Pass', 'Fail', 'Abort', 'ReadyToAbort']:
            pass
        else:
            raise Exception('Unexpected state: ' + self._state_by_jobId[jobId])

    def _set_final_pipeline_state(self, pipelineId):
        for jobId in self._jobIds_by_pipelineId[pipelineId]:
            if self._state_by_jobId[jobId] != 'Pass':
                self._state_by_pipelineId[pipelineId] = 'Fail'
                break
        else:
            self._state_by_pipelineId[pipelineId] = 'Pass'
        # Purge most of the pipeline information, preserving just the state of the pipeline and jobs.
        
        pipeline_jobs = self._jobIds_by_pipelineId[pipelineId]
        for jobId in pipeline_jobs:
            job_outfiles = self._outfiles_by_jobId[jobId]
            for job_outfile in job_outfiles:
                del self._jobId_by_outfile[job_outfile]
            del self._outfiles_by_jobId[jobId]
            del self._pipelineId_by_jobId[jobId]
            del self._pendingParentJobs_by_jobId[jobId]
            del self._pendingChildJobs_by_jobId[jobId]
            del self._cleanUpPipelineJobsOnFail_by_jobId[jobId]
        del self._jobIds_by_pipelineId[pipelineId]
        del self._pendingJobIds_by_pipelineId[pipelineId]        

    def startJob(self, jobId):
        if self._state_by_jobId[jobId] != 'ReadyToStart':
            raise Exception('Job must be ReadyToStart before it can be move to Started')
        self._state_by_jobId[jobId] = 'Started'

    def killJob(self, jobId):
        if jobId not in self._pipelineId_by_jobId:
            raise Exception('kill job called on a job that was never initialized')
        pipelineId = self._pipelineId_by_jobId[jobId]
        if jobId not in self._pendingJobIds_by_pipelineId[pipelineId]:
            raise Exception('kill job called on a job which has already been terminated: ' + jobId)

        if self._state_by_jobId[jobId] != 'ReadyToAbort':
            raise Exception('Job must be ReadyToAbort before it can be killed')
        
        self._state_by_jobId[jobId] = 'Abort'
        self._pendingJobIds_by_pipelineId[pipelineId].remove(jobId)
        if not self._pendingJobIds_by_pipelineId[pipelineId]:
            self._set_final_pipeline_state(pipelineId)

    def purgeJob(self, jobId):
        pass

    def getJobState(self, jobId):
        jobState = self._state_by_jobId[jobId]

        #print ('state: '+jobState+' job: '+jobId)
        #print (str(self._pendingJobIds_by_pipelineId))
        return jobState

    def getPipelineState(self, pipelineId):
        return self._state_by_pipelineId[pipelineId]

    def getPipelineStates(self):
        pipelineStateDetails_by_pipelineId = {}
        pipelineJobs_by_pipelineId = {}
        pipelineState_by_pipelineId=self._state_by_pipelineId.copy()
        for pipelineId in self._state_by_pipelineId:
            if self._state_by_pipelineId[pipelineId] != 'InProgress':
                continue
            state_counter = collections.Counter()
            pipeline_jobs = self._jobIds_by_pipelineId[pipelineId].copy()
            pipelineJobs_by_pipelineId[pipelineId] = pipeline_jobs
            for jobId in pipeline_jobs:
                state_counter[self._state_by_jobId[jobId]] += 1
            pipelineStateDetails_by_pipelineId[pipelineId] = state_counter
        return (pipelineStateDetails_by_pipelineId, pipelineJobs_by_pipelineId, pipelineState_by_pipelineId)

class Priority:
    def __init__(self):
        self._pipelineId_by_jobId = {}
        self._pending_jobIds_by_pipelineId = {}
        self._jobIds_by_pipelineId = {}
        self._pending_jobIds_by_pipelineId = {}
        self._outFiles_by_jobId = {}
        self._jobId_by_outFile = {}
        self._pipelineTimestamp_by_jobId = {}
        self._pipelinePriority_by_jobId = {}
        self._ancestors_by_jobId = {}
        self._downstreamTime_by_jobId = collections.Counter()
        self._jobActive_by_jobId = {}
        

    def newJob(self, jobId, pipelineId, inFiles, outFiles, pipelineTimestamp, pipelinePriority, jobTime):
        if int(pipelinePriority) < 0 or int(pipelinePriority) > 100:
            raise Exception('pipeline priority out of range: ' + str(pipelinePriority))
        
        self._pipelineId_by_jobId[jobId] = pipelineId
        if pipelineId not in self._jobIds_by_pipelineId:
            self._jobIds_by_pipelineId[pipelineId] = []
            self._pending_jobIds_by_pipelineId[pipelineId] = set()
        self._jobIds_by_pipelineId[pipelineId].append(jobId)
        self._pending_jobIds_by_pipelineId[pipelineId].add(jobId)

        self._outFiles_by_jobId[jobId] = outFiles
        for outFile in outFiles:
            self._jobId_by_outFile[outFile] = jobId
        self._pipelineTimestamp_by_jobId[jobId] = pipelineTimestamp
        self._pipelinePriority_by_jobId[jobId] = pipelinePriority
        
        # keep track of ancestors as a unique set, to avoid double counting
        self._ancestors_by_jobId[jobId] = set()
        for inFile in inFiles:
            self._ancestors_by_jobId[jobId].add(self._jobId_by_outFile[inFile])
            parent_ancestors = self._ancestors_by_jobId[self._jobId_by_outFile[inFile]]
            self._ancestors_by_jobId[jobId].update(parent_ancestors)
                
        self._downstreamTime_by_jobId[jobId] += int(jobTime) # include self
        for ancestorJobId in self._ancestors_by_jobId[jobId]:
            self._downstreamTime_by_jobId[ancestorJobId] += int(jobTime)
            
        self._jobActive_by_jobId[jobId] = True

    def purgeJob(self, jobId):
        self._jobActive_by_jobId[jobId] = False
        pipelineId = self._pipelineId_by_jobId[jobId]
        self._pending_jobIds_by_pipelineId[pipelineId].remove(jobId)
        if not self._pending_jobIds_by_pipelineId[pipelineId]:
            #self._finishPipeline(pipelineId) #Hack to permit wait statement to not trash the pipeline object
            print ("pipeline " + pipelineId + " now has zero jobs")

    def _finishPipeline(self, pipelineId):
        jobIds = self._jobIds_by_pipelineId[pipelineId]
        for jobId in jobIds:
            del self._pipelineId_by_jobId[jobId]
            outFiles = self._outFiles_by_jobId[jobId]
            for outFile in outFiles:
                del self._jobId_by_outFile[outFile]
            del self._outFiles_by_jobId[jobId]
            del self._pipelineTimestamp_by_jobId[jobId]
            del self._pipelinePriority_by_jobId[jobId]
            del self._ancestors_by_jobId[jobId]
            del self._downstreamTime_by_jobId[jobId]
            del self._jobActive_by_jobId[jobId]
        del self._jobIds_by_pipelineId[pipelineId]
        
    def setPipelinePriority(self, pipelineId, pipelinePriority):
        if int(pipelinePriority) < 0 or int(pipelinePriority) > 100:
            raise Exception('pipeline priority out of range: ' + str(pipelinePriority))

        jobIds = self._jobIds_by_pipelineId[pipelineId]
        for jobId in jobIds:
            self._pipelinePriority_by_jobId[jobId]=pipelinePriority
            
    def getPipelinePriorities(self):
        pipelinePriorities_by_pipelineId = {}
        for pipelineId in self._jobIds_by_pipelineId:
            jobIds = self._jobIds_by_pipelineId[pipelineId]
            pipelinePriority = self._pipelinePriority_by_jobId[jobIds[0]]
            pipelinePriorities_by_pipelineId[pipelineId] = pipelinePriority
        
        return pipelinePriorities_by_pipelineId
    
    def getJobPriorities(self):
        jobId_by_jobHash = {}
        for jobId in self._jobActive_by_jobId:
            if not self._jobActive_by_jobId[jobId]:
                continue
            # Lower jobHash values mean higher priority.
            # Make each individual hash a uniform length, except for jobId at end for uniqueness
            pipelinePriority_hash = str(100 - int(self._pipelinePriority_by_jobId[jobId])).zfill(3)
            pipelineTimestamp_hash = self._pipelineTimestamp_by_jobId[jobId]
            downstreamTime_hash = str(int(1e8 - self._downstreamTime_by_jobId[jobId])).zfill(9)
            jobHash = '.'.join([pipelinePriority_hash, pipelineTimestamp_hash, downstreamTime_hash, jobId])
            jobId_by_jobHash[jobHash]=jobId

        prioritized_jobIds = []
        for jobHash in sorted(jobId_by_jobHash):
            prioritized_jobIds.append(jobId_by_jobHash[jobHash])
            
        #print (str(prioritized_jobIds))
        return prioritized_jobIds
             

class Report:
    def __init__(self, communicationDirBase, jobs, state, priority, jobDirectoryManager, executionEngines):
        self._communicationDirBase = communicationDirBase
        self._jobs = jobs
        self._state = state
        self._priority = priority
        self._jobDirectoryManager = jobDirectoryManager
        self._executionEngines = executionEngines
        self._report_dir = os.path.join(self._communicationDirBase,'report')
        self._system = platform.system()
        self._log_state_pass=0

    def dump_pipeline_status(self):
        (pipelineStateDetails_by_pipelineId, pipelineJobs_by_pipelineId, pipelineState_by_pipelineId) = \
        self._state.getPipelineStates()
        
        status = self._build_pipeline_status(pipelineState_by_pipelineId)
        pipeline_status_path = os.path.join(self._report_dir, 'pipeline_only.status.txt')
        self._write_status_file(pipeline_status_path, status)
        
        status = self._build_pipeline_summary_status(pipelineState_by_pipelineId, pipelineStateDetails_by_pipelineId)
        pipeline_summary_status_path = os.path.join(self._report_dir, 'pipeline_summary.status.txt')
        self._write_status_file(pipeline_summary_status_path, status)

        status = self._build_job_status(pipelineState_by_pipelineId, pipelineJobs_by_pipelineId,
                                        self._executionEngines, self._jobDirectoryManager)
        job_status_path = os.path.join(self._report_dir, 'job.status.txt')
        self._write_status_file(job_status_path,status)
        
    def log_state(self, msg, state_path=None):
        (pipelineStateDetails_by_pipelineId, pipelineJobs_by_pipelineId, pipelineState_by_pipelineId) = \
        self._state.getPipelineStates()
        if state_path is None:
            state_path = os.path.join(self._report_dir, 'job_state.log.txt')

        status = self._build_job_status(pipelineState_by_pipelineId, pipelineJobs_by_pipelineId,
                                        self._executionEngines, self._jobDirectoryManager)
        self._log_state_pass += 1
        self._append_log_file(state_path,status, self._log_state_pass, msg)

    def reset_state_passnum(self):
        self._log_state_pass = 0

    def _append_log_file(self, status_path, status_contents, passnum, msg):
        if True:
            return
        status_dir = os.path.dirname(status_path)
        os.makedirs(status_dir,exist_ok=True)

            
        fid = open(status_path, 'at')
        writer = csv.writer(fid, dialect='excel-tab', lineterminator = '\n')
        writer.writerow(['-----------------------------------------------------------'])
        writer.writerow(['Pass: ' + str(passnum) + '   ' + msg])
        writer.writerow(['-----------------------------------------------------------'])
        writer.writerows(status_contents)
        fid.close()

    def _write_status_file(self, status_path, status_contents):
        # TODO - make this instead toggle between two file names, and update a symlink on linux.
        status_tmp_path = status_path + '.tmp'
        status_dir = os.path.dirname(status_path)
        os.makedirs(status_dir,exist_ok=True)

    
        fid = open(status_tmp_path, 'wt')
        writer = csv.writer(fid, dialect='excel-tab', lineterminator = '\n')
        writer.writerows(status_contents)
        fid.close()
        try: 
            if self._system == 'Windows':
                if os.path.exists(status_path):
                    os.remove(status_path)
            elif self._system == 'Linux':
                pass
            else:
                raise Exception('unsupported platform: ' + self._system)
            os.rename(status_tmp_path, status_path)
        except:
            print('failed to update status file at ' + status_path)
            
       
    def _build_pipeline_status(self, pipelineState_by_pipelineId):
        status = []
        # Output a 2 column table containing pipeline id and status
        status.append(['PipelineStatus', 'PipelineId'])
        for pipelineId in sorted(pipelineState_by_pipelineId):
            status.append([pipelineState_by_pipelineId[pipelineId], pipelineId])
        return status
        
    def _build_pipeline_summary_status(self, pipelineState_by_pipelineId, pipelineStateDetails_by_pipelineId):
        status = []
        status.append(['PipelineStatus', 'PipelineName', 'WaitingForDependencies',
                         'ReadyToStart', 'Started', 'Pass', 'ReadyToAbort', 'Abort', 'Fail'])
        for pipelineId in sorted(pipelineState_by_pipelineId):
            if pipelineId in pipelineStateDetails_by_pipelineId:
                summDict = pipelineStateDetails_by_pipelineId[pipelineId]
                status.append([pipelineState_by_pipelineId[pipelineId], pipelineId, summDict['WaitingForDependencies'],
                               summDict['ReadyToStart'], summDict['Started'], summDict['Pass'],
                               summDict['ReadyToAbort'], summDict['Abort'], summDict['Fail']])
            else:
                status.append([pipelineState_by_pipelineId[pipelineId], pipelineId, '-', '-', '-', '-', '-', '-', '-'])
                
        return status
        
    def _build_job_status(self, pipelineState_by_pipelineId, pipelineJobs_by_pipelineId, executionEngines,
                          jobDirectoryManager):
        status = []
        status.append(['ModuleType', 'ModuleStatus', 'EngineStatus', 'EngineJobId', 'DirectoryStatus', 'ModuleName'])
        for pipelineId in sorted(pipelineState_by_pipelineId):
            status.append(['Pipeline', pipelineState_by_pipelineId[pipelineId], '-', '-', '-', pipelineId])
            if pipelineId in pipelineJobs_by_pipelineId:
                # print jobs only for active pipelines
                for jobId in sorted(pipelineJobs_by_pipelineId[pipelineId]):
                    engineStatus = executionEngines.getJobStatus(jobId)
                    engineJobId = executionEngines.getEngineJobId(jobId)
                    dirStatus = jobDirectoryManager._get_module_directory_status(jobId)
                    if dirStatus == jobId:
                        dirStatus = 'jobId'
                    jobState = self._state.getJobState(jobId)
                    status.append(['Module', jobState, engineStatus, engineJobId, dirStatus, jobId])
        return status
        
        
class ObjectFactoryStore:
    def __init__(self, communicationDirBase, scriptDir):
        self._communicationDirBase = communicationDirBase
        self._scriptDir = scriptDir
        # _objDict is the top level object, with references to all the others directly or indirectly.
        self._objDict = {}
        self._storePath = os.path.join(self._communicationDirBase, 'state', 'pipette_store.p')
        self._tmpStorePath = self._storePath + '.tmp'
        
    def storeExists(self):
        value = os.path.exists(self._storePath) or os.path.exists(self._tmpStorePath)
        return value

    def loadFromSavedState(self):
        if os.path.exists(self._storePath):
            fpath = self._storePath
        elif os.path.exists(self._tmpStorePath):
            fpath = self._tmpStorePath
        else:
            raise Exception('Could not find existing pipette_store in ' + self._communicationDirBase)
        fid = open(fpath, 'rb')
        self._objDict = pickle.load(fid)
        return self._objDict
        
    def initFresh(self, do_retry):
        # Create list of all possible execution engines
        engineDict = {}
        engineDict['local'] = LocalEngine.LocalEngine()
        
        # Pass list of execution engines into abstract wrapper for them.
        executionEngines = ExecutionEngines(engineDict, self._scriptDir, do_retry)
        
        # Instantiate other worker objects
        jobDirectoryManager = JobDirectoryManager(do_retry)
        jobs = Jobs(self._communicationDirBase)
        priority = Priority()
        state = State()
        
        # Pass all objects into Report, which should only read and not modify them
        report = Report(self._communicationDirBase, jobs, state, priority, jobDirectoryManager, executionEngines)

        # Bundle all of these objects into a single dict
        self._objDict['executionEngines'] = executionEngines
        self._objDict['jobDirectoryManager'] = jobDirectoryManager
        self._objDict['jobs'] = jobs
        self._objDict['priority'] = priority
        self._objDict['state'] = state
        self._objDict['report'] = report
        
        # Do the initial saving of state
        self.saveState()
        
        # Pass this dict out, for use in main routine
        return self._objDict

    def saveState(self):
        file_dir = os.path.dirname(self._storePath)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir,exist_ok=True)
            fid = open(self._storePath, 'w')
            fid.close()

        fid = open(self._tmpStorePath, 'wb')
        try:
            pickle.dump(self._objDict, fid, -1)
        except:
            fid.close()
            os.remove(self._tmpStorePath)
            raise
        fid.close()
        if os.path.exists(self._storePath):
            os.remove(self._storePath) # This remove step is the atomic commit.
        os.rename(self._tmpStorePath, self._storePath)

        
class FileDeletionQueue:
    
    def __init__(self):
        self.del_queue = self.deletion_coroutine()
        # prime the coroutine
        next(self.del_queue)
        print("Deletion Queue Started .....")

    def deletion_coroutine(self):
        files = queue.Queue()

        def deletion_thread():
            while True:
                cur_file = files.get()
                if cur_file is GeneratorExit:
                    print("Deletion Queue Exiting .....")
                    return
                else:
                    cur_file_path = os.path.expanduser(cur_file)
                    print("Deletion Queue Removing " + cur_file_path + " .....")
                    try:
                        os.remove(os.path.expanduser(cur_file_path))
                    except OSError:
                        # File doesn't exist, ignore.
                        pass
                    
        t = threading.Thread(target=deletion_thread)
        t.daemon = True
        t.start()
        while True:
            new_file = (yield)
            files.put(new_file)

    def add_file(self, file_name):
        self.del_queue.send(file_name)

    def halt_coroutine(self):
        # Will send a signal to the coroutine to cause it to halt. This will not brutally
        # kill the thread however, it will work through the full queue.
        self.del_queue.send(GeneratorExit)


class Main:
    def __init__(self):
        self._fileDeletionQueue = FileDeletionQueue()
        # Make sure that the deletion queue thread halts when the interpreter is shutting down, otherwise
        # the exit might hang
        atexit.register(self._fileDeletionQueue.halt_coroutine)
        self._nothingFailed = True

    def do_iteration(self, objDict):
        executionEngines = objDict['executionEngines']
        jobDirectoryManager = objDict['jobDirectoryManager']
        jobs = objDict['jobs']
        priority = objDict['priority']
        state = objDict['state']
        report = objDict['report']
        if False:
            # Add these statements to give type hints in the debugger
            isinstance(executionEngines, ExecutionEngines)
            isinstance(jobDirectoryManager, JobDirectoryManager)
            isinstance(jobs, Jobs)
            isinstance(priority, Priority)
            isinstance(state, State)
            isinstance(report, Report)
        test_dump_status = True

        if test_dump_status:
            report.log_state('Starting iteration')
            
        self.fetch_new_jobs(jobs, state, priority, jobDirectoryManager, executionEngines)
        # Don't bother touching WaitingForDependencies jobs... they will advance as a side effect when ready.

        if test_dump_status:
            report.log_state('fetched new jobs')

        prioritized_jobIds = priority.getJobPriorities()
        executionEngines.update(prioritized_jobIds)

        taskmaster_counter = 0
        for jobId in prioritized_jobIds:
            if state.getJobState(jobId) == 'ReadyToStart':
                # Limit how long to spend on this task
                taskmaster_counter += 1
                if taskmaster_counter > 1000:
                    break
                self.process_ready_to_start_job(jobId, jobs, state, priority, jobDirectoryManager, executionEngines)
                
        if test_dump_status:
            report.log_state('processed ready to start')

        taskmaster_counter = 0
        for jobId in prioritized_jobIds:
            if state.getJobState(jobId) == 'Started':
                # Limit how long to spend on this task
                taskmaster_counter += 1
                if taskmaster_counter > 1000:
                    break
                self.process_started_job(jobId, jobs, state, priority, jobDirectoryManager, executionEngines)

        if test_dump_status:
            report.log_state('processed started')

        taskmaster_counter = 0
        for jobId in prioritized_jobIds:
            if state.getJobState(jobId) == 'ReadyToAbort':
                # Do not limit how long to spend on this task
                taskmaster_counter += 1
                if taskmaster_counter > 1000:
                    pass
                self.process_ready_to_abort_job(jobId, jobs, state, priority, jobDirectoryManager, executionEngines)

        if test_dump_status:
            report.log_state('processed aborted')

        taskmaster_counter = 0
        for jobId in prioritized_jobIds:
            if state.getJobState(jobId) in ['Pass', 'Fail', 'Abort']:
                # Limit how long to spend on this task
                taskmaster_counter += 1
                if taskmaster_counter > 1000:
                    break
                self.process_passed_failed_aborted_job(jobId, jobs, state, priority, jobDirectoryManager,
                                                       executionEngines)

        if test_dump_status:
            report.log_state('processed pass/fail/abort jobs')
            
        report.dump_pipeline_status()

    def fetch_new_jobs(self, jobs, state, priority, jobDirectoryManager, executionEngines):
        if False:
            # Add these statements to give type hints in the debugger
            isinstance(executionEngines, ExecutionEngines)
            isinstance(jobDirectoryManager, JobDirectoryManager)
            isinstance(jobs, Jobs)
            isinstance(priority, Priority)
            isinstance(state, State)
        jobfiles = jobs.get_new_jobfiles()
        jobfiles.sort()
        # load each file off of the disk, load it into our various data structures
        taskmaster_counter = 0
        for jobfile in jobfiles:
            # Limit how long to spend on this task
            taskmaster_counter += 1
            if taskmaster_counter > 1000:
                break
            # TODO should this loop be explicitly time limited, to keep things from bogging down if thousands 
            # of jobs are submitted at once?
            jobIds = jobs.load_new_job(jobfile)
            for jobId in jobIds:
                jobDict = jobs.get_job(jobId)
                
                state.newJob(jobId=jobId, pipelineId=jobDict['pipelineId'],inFiles=jobDict['inFiles'],
                             outFiles=jobDict['outFiles'], cleanUpPipelineJobsOnFail=jobDict['cleanUpPipelineJobsOnFail'])
                
                priority.newJob(jobId=jobId, pipelineId=jobDict['pipelineId'], inFiles=jobDict['inFiles'],
                                outFiles=jobDict['outFiles'], pipelineTimestamp=jobDict['pipelineTimestamp'],
                                pipelinePriority=jobDict['pipelinePriority'], jobTime=jobDict['resources']['maxtime'])
                
                jobDirectoryManager.newJob(jobId=jobId, moduleOutDir=jobDict['moduleOutDir'],
                                           cleanUpJobFilesOnFail=jobDict['cleanUpJobFilesOnFail'])
                
                job_outdir = jobDirectoryManager.get_job_directory(jobId)
                executionEngines.newJob(jobId=jobId, pipelineName=jobDict['pipelineName'],
                                        engineName=jobDict['executionEngine'], cmdStr=jobDict['cmdStr'],
                                        resources=jobDict['resources'], job_outdir=job_outdir)
                
    def process_ready_to_start_job(self, jobId, jobs, state, priority, jobDirectoryManager, executionEngines):
        print ('process ready_to_start ' + jobId)
        if False:
            # Add these statements to give type hints in the debugger
            isinstance(executionEngines, ExecutionEngines)
            isinstance(jobDirectoryManager, JobDirectoryManager)
            isinstance(jobs, Jobs)
            isinstance(priority, Priority)
            isinstance(state, State)

        # If job already passed, and this job allows caching, then move the job directly to the Pass state.
        jobDict = jobs.get_job(jobId)
        if jobDict['caching'] == 'True':
            dir_status = jobDirectoryManager.get_directory_pass_status(jobId)
            if dir_status == 'PASS':
                state.passJob(jobId)
                return
        elif jobDict['caching'] == 'False':
            pass
        else:
            raise Exception('unexpected value for caching: ' + jobDict['caching'])
        
        # Commence launching the job
        jobDirectoryManager.create_fresh_job_directory(jobId)
        state.startJob(jobId)
        executionEngines.launchJob(jobId)
            
    def process_started_job(self, jobId, jobs, state, priority, jobDirectoryManager, executionEngines):
        #print ('process started: '+jobId)
        if False:
            # Add these statements to give type hints in the debugger
            isinstance(executionEngines, ExecutionEngines)
            isinstance(jobDirectoryManager, JobDirectoryManager)
            isinstance(jobs, Jobs)
            isinstance(priority, Priority)
            isinstance(state, State)
        status = executionEngines.getJobStatus(jobId)
        if status in ['QUEUED','RUNNING']:
            return
        elif status == 'PASS':
            dirCleanupStatus = jobDirectoryManager.cleanup_for_passing(jobId)
            if dirCleanupStatus == 'Pass':
                state.passJob(jobId)
                jobDict = jobs.get_job(jobId)
                for fileToBeDeleted in jobDict['deleteFiles']:
                    self._fileDeletionQueue.add_file(fileToBeDeleted)
            elif dirCleanupStatus == 'Abort':
                state.abortJob(jobId)
            else:
                raise Exception ('Unexpected dirCleanupStatus: ' + dirCleanupStatus)

        elif status == 'FAIL':
            dirCleanupStatus = jobDirectoryManager.cleanup_for_failing(jobId)
            if dirCleanupStatus == 'Pass':
                state.failJob(jobId)
            elif dirCleanupStatus == 'Abort':
                state.abortJob(jobId)
            else: 
                raise Exception('Unexpected dirCleanupStatus: ' + dirCleanupStatus)
            
        elif status == 'ERROR':
            state.abortJob(jobId)
        else:
            raise Exception('unrecognized engine job status: ' + status)
        
        
    def process_ready_to_abort_job(self, jobId, jobs, state, priority, jobDirectoryManager, executionEngines):
        print ('process ready_to_abort: ' + jobId)
        if False:
            # Add these statements to give type hints in the debugger
            isinstance(executionEngines, ExecutionEngines)
            isinstance(jobDirectoryManager, JobDirectoryManager)
            isinstance(jobs, Jobs)
            isinstance(priority, Priority)
            isinstance(state, State)

        # kill job, if it is still running
        state.killJob(jobId) 
        executionEngines.killJob(jobId)
        dirCleanupStatus = jobDirectoryManager.cleanup_for_aborting(jobId)
        # Cannot act on dirCleanupStatus, any more than we have already done...

    def process_passed_failed_aborted_job(self, jobId, jobs, state, priority, jobDirectoryManager, executionEngines):
        print (state.getJobState(jobId) + ' ' + jobId)
        if False:
            # Add these statements to give type hints in the debugger
            isinstance(executionEngines, ExecutionEngines)
            isinstance(jobDirectoryManager, JobDirectoryManager)
            isinstance(jobs, Jobs)
            isinstance(priority, Priority)
            isinstance(state, State)
        state.purgeJob(jobId)
        priority.purgeJob(jobId)
        jobDirectoryManager.purge_job(jobId)
        executionEngines.purgeJob(jobId)
        jobs.purge_job(jobId)
        self._nothingFailed = False

    def _flush_commDir(self, communicationDirBase):
        launch_dir = os.path.join(communicationDirBase,'launch')
        archive_dir = os.path.join(communicationDirBase,'archive')
        state_dir = os.path.join(communicationDirBase,'state')
        report_dir = os.path.join(communicationDirBase,'report')
        if os.path.exists(launch_dir):
            shutil.rmtree(launch_dir)
        if os.path.exists(archive_dir):
            shutil.rmtree(archive_dir)
        if os.path.exists(state_dir):
            shutil.rmtree(state_dir)
        if os.path.exists(report_dir):
            shutil.rmtree(report_dir)       
                                       
    def run_server(self, communicationDirBase, scriptDir, runMode, retryMode):
        
        if runMode == 'server':
            do_flush = True
        elif runMode == 'runone':
            do_flush = False
        else:
            raise Exception('unknown value for run_mode - must be "server" or "runone"')

        if retryMode == 'True':
            do_retry = True
        elif retryMode == 'False':
            do_retry = False
        else:
            raise Exception('unknown value for retryMode - must be "True" or "False"')
        
        if do_flush:
            self._flush_commDir(communicationDirBase)
            
        
        objectFactoryStore = ObjectFactoryStore(communicationDirBase, scriptDir)
        if False and objectFactoryStore.storeExists():
            objDict = objectFactoryStore.loadFromSavedState()
        else:
            objDict = objectFactoryStore.initFresh(do_retry)
        print ('Pipette Server is awake')
            
        # don't do iterations more often than min_iteration_time seconds
        min_iteration_time = 10
        pipeline_started = False
        while True:
            start_time = self._get_time()
            self.do_iteration(objDict)
            objectFactoryStore.saveState()

            (pipelineStateDetails_by_pipelineId, pipelineJobs_by_pipelineId,
               pipelineState_by_pipelineId) = objDict['state'].getPipelineStates()

            num_active_pipelines = 0
            for pipelineId in pipelineState_by_pipelineId:
                if pipelineState_by_pipelineId[pipelineId] == 'InProgress':
                    num_active_pipelines += 1
            if pipeline_started:
                if not num_active_pipelines:
                    if runMode == 'server':
                        pass #Stay in infinite loop
                    elif runMode == 'runone':
                        return(self._nothingFailed)
                        #Halt once initial pipelines are done
                    else:
                        raise Exception('unknown run_mode value - must be "server" or "runone"')    
            else:
                if num_active_pipelines:
                    pipeline_started = True
                elif runMode == 'runone':
                    raise Exception('No pipelines submitted for execution')
            
            end_time = self._get_time()
            time_to_sleep = min_iteration_time - (end_time - start_time)
            if time_to_sleep>0:
                time.sleep(time_to_sleep)
       
    def _get_time(self):
        t = datetime.datetime.now()
        t_days = t.toordinal()
        t_secs = ((t_days - 73300) * 24 * 60 * 60) + (t.hour * 60 * 60) + (t.minute * 60) + t.second
        return t_secs


    
def main():
    platform_name = platform.system()
    if platform_name == 'Windows':
        defaultCommunicationDirBase = "c:\Documents and Settings\G\My Documents\pipette_code\commdir"
    elif platform_name == 'Linux':
        #defaultCommunicationDirBase = os.path.join('/broad','hptmp',username,'pipette_comm')
        home = os.environ['HOME']
        defaultCommunicationDirBase = os.path.join(home,'pipette_queue')

    else:
        raise Exception('unknown platform type')    
    
    
    #full_script_path = os.path.abspath(sys.argv[0])
    full_script_path = os.path.abspath(__file__)
    scriptDir = os.path.dirname(full_script_path)
    
    parser = optparse.OptionParser()
    parser.add_option( "--run_mode", dest="run_mode",default='server',help="Should be 'server' or 'runone'")
    parser.add_option( "--retry_mode", dest="retry_mode",default='True',help="Should be 'True' or 'False'")

    parser.add_option( "-c","--comm_dir", dest="comm_dir",
                       default=defaultCommunicationDirBase,help="Storage for state files")
    (options, args) = parser.parse_args()

    runMode = options.run_mode
    retryMode = options.retry_mode
    communicationDirBase = options.comm_dir

    print ("Communications Directory:"+options.comm_dir)



    main_object = Main()
    nothingFailed = main_object.run_server(communicationDirBase, scriptDir, runMode, retryMode)
    if not nothingFailed:
        raise Exception('One or more pipelines failed.')

if __name__ == '__main__':
    main()
    
    #cProfile.run('main.run_server(communicationDirBase,scriptDir, run_mode)','/xchip/tcga_scratch/gsaksena/test_pipette/profile_output.txt')
