__author__ = 'gsaksena'

import sys
import os
import subprocess

pipeline_outdir = sys.argv[1]  # should be $PIPELINEOUTDIR
link_outdir = sys.argv[2]      # place to dump the links.  put under $PIPELINEOUTDIR to segregate runs, or outside of it to consolidate.  probably do not list as output files
input_paths = sys.argv[3:]     # files to link to.  should also be listed as input files



for input_path in input_paths:
    if not os.path.exists(input_path):
        raise Exception('input file does not yet exist: %s'%input_path)
    if not input_path.startswith(pipeline_outdir):
        raise Exception('input file does not live under $PIPELINEOUTDIR: %s not under %s'%(input_path,pipeline_outdir))


    #remove base directory name and leading slash
    input_subpath = input_path[(len(pipeline_outdir)+1):]
    input_subdir = os.path.dirname(input_subpath)
    input_fn = os.path.basename(input_subpath)

    if '.gz' in input_fn or '.bam' in input_fn:
        do_compression = False
    else:
        do_compression = True

    output_subdir = input_subdir.replace('/','_')
    output_fn = input_fn
    if do_compression:
        output_fn += '.gz'
    output_path = os.path.join(link_outdir, output_subdir, output_fn)
    output_dir = os.path.dirname(output_path)

    #remove any existing link or file at this location, to be idempotent
    if os.path.lexists(output_path):
        os.remove(output_path)


    try:
        os.makedirs(output_dir)
    except:
        #catch case where directory already exists
        pass


    if not do_compression:
        print ('creating symlink %s'%output_path)
        os.symlink(input_path, output_path)
    else:
        cmdstr ='gzip -c %s > %s'%(input_path, output_path)
        print('zipping file to %s'%output_path)
        subprocess.check_call(cmdstr, shell=True)