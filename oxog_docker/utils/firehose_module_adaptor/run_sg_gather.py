#!/usr/bin/env python
import os
import sys
import cga_util
import common
#import argparse
import run_module
import optparse

def run_gather(module_libdir, prepare_outdir, scatter_outdirs, cwd):
    gather_cmd = common.get_gather_cmd(module_libdir)
    prepare_out_fn = os.path.join(prepare_outdir,'prepareResults.out')
    prepare_out_str = cga_util.read_string_from_file(prepare_out_fn,'rstrip')
    prepare_out_list = prepare_out_str.split('\n')
    gather_args = prepare_out_list[-1]

    
    #TBD map to correct environment
    #if '.py' in gather_cmd:
     #   gather_cmd = 'source /broad/software/scripts/useuse; use Python-2.7; ' + gather_cmd
    
    if cwd is None:
        cwd = os.getcwd()
    for s_dir in scatter_outdirs:
        base = os.path.basename(s_dir)
        gather_s_dir = os.path.join(cwd,base)
        cga_util.safe_make_symlink(s_dir, gather_s_dir)
        #os.symlink('../../../'+base,base)
    
    cmd_str = gather_cmd + ' ' + gather_args

    print ('executing: %s'%cmd_str)
    
    passing = run_module.execute_command_like_gp(cmd_str, cwd, logparams=None, error_mode='exit_code')    
    
    if not passing:
        raise Exception('gather step failed')
    

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('usage: run_sg_gather.py --module_libdir <modulelibdir> --prepare_outdir <prepare_outdir> --scatter_outdir <scatter_outdir1> --scatter_outdir <scatter_outdir2> [--cwd  <outputdir>] ')

    print ('python version '+sys.version)
    
    
    parser = optparse.OptionParser()
    parser.add_option("--scatter_outdir", dest="scatter_outdir",action='append',
                      help="outdir for scatter steps. include arg multiple times")
    parser.add_option("--module_libdir", dest="module_libdir",
                      help="module source code location")
    parser.add_option("--prepare_outdir", dest="prepare_outdir",
                      help="outdir for prepare step")
    parser.add_option("--cwd", dest="cwd",
                      help="working directory for output, defaults to current directory")
    
    (args, args_other) = parser.parse_args()        
    
    
    
    #parser = argparse.ArgumentParser(description='Run one scatter step of a scatter gather module')

    #parser.add_argument('--module_libdir',
                       #help='module source code location')
    #parser.add_argument('--prepare_outdir',
                       #help='outdir for prepare step')
    #parser.add_argument('--scatter_outdir', action='append',
                       #help='outdir for scatter steps. include arg multiple times')
    #parser.add_argument('--cwd',default=None,
                       #help='working directory for output, defaults to current directory')
    
    #args = parser.parse_args()
    run_gather(args.module_libdir, args.prepare_outdir, args.scatter_outdir, args.cwd)