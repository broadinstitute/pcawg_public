#!/usr/bin/python

import tarfile
import os
import datetime
import subprocess


def get_timestamp():
    t=datetime.datetime.now()
    timestamp = '%d_%02d_%02d__%02d_%02d_%02d'%(t.year,t.month,t.day,t.hour,t.minute,t.second)
    return timestamp


def dopackage(basedir, outdir, timestamp, input_subdirs, archivename):
    tarfile_dir = os.path.join(outdir, 'bundle_' + timestamp)
    
    tarfile_path = os.path.join(tarfile_dir, archivename + '.tar')
    try: 
        os.makedirs(tarfile_dir)
    except:
        pass
    
    with tarfile.open(tarfile_path, mode='w') as tf:
        for input_subdir in input_subdirs: 
            input_dir = os.path.join(basedir, input_subdir)            
            tf.add(input_dir, input_subdir)
    cmdstr = 'gzip --fast ' + tarfile_path
    subprocess.check_call(cmdstr, shell=True)
    cmdstr = 'openssl aes-128-cbc -salt -in %s.gz -out %s.gz.aes -k hixhhktkfgfgkhhdihti'%(tarfile_path,tarfile_path)
    subprocess.check_call(cmdstr, shell=True)

    #decrypt via eg
    #openssl aes-128-cbc -d -salt -in docker2 -out docker2.tar.gz
    #enter pw
            
basedir = '/cga/fh/pcawg_pipeline4'
outdir = '/cga/fh/pcawg_pipeline4/export'
timestamp = get_timestamp()

#code
dopackage(basedir, outdir, timestamp, ['docker'], 'docker10')

#ref data
#note - cannot be sent unencrypted, includes protected data
#dopackage(basedir, outdir, timestamp, ['refdata'], 'refdata9')

#dopackage(basedir, outdir, timestamp, ['refdata/public'], 'refdata9_public')



