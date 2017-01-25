#!/usr/bin/env python
import os
import sys
import cga_util
import common
#import argparse
import run_module

import optparse


def run_scatter(module_libdir, prepare_outdir, scatter_index, cwd):
    scatter_cmd = common.get_scatter_cmd(module_libdir)
    prepare_out_fn = os.path.join(prepare_outdir,'prepareResults.out')
    prepare_out_str = cga_util.read_string_from_file(prepare_out_fn,'rstrip')
    prepare_out_list = prepare_out_str.split('\n')
    prepare_out_list = prepare_out_list[:-1] #drop gather line
    scatter_index_int = int(scatter_index)
    scatter_args = prepare_out_list[scatter_index_int-1]
    
    #if some file lives in the prepare directory, ensure string says it is right under it
    scatter_args_list = scatter_args.split()
    scatter_args_list2 = []
    
    print ('prepare outdir: ' + prepare_outdir)
    
    for s_arg in scatter_args_list:
        if s_arg.startswith(prepare_outdir):
            print ('shortening arg...' + s_arg)
            s_arg = os.path.join(prepare_outdir,os.path.basename(s_arg))
        elif s_arg.startswith('"' + prepare_outdir):
            #handle case where filename is surrounded by ""
            s_arg = os.path.join('"' + prepare_outdir,os.path.basename(s_arg) )
        scatter_args_list2.append(s_arg)
    scatter_args2 = ' '.join(scatter_args_list2)
                                 
    
    
    cmd_str = scatter_cmd + ' ' + scatter_args2
    if cwd is None:
        cwd = os.getcwd()
    print ('executing: %s'%cmd_str)
    
    passing = run_module.execute_command_like_gp(cmd_str, cwd, logparams=None, error_mode='exit_code')    
    
    if not passing:
        raise Exception('scatter step failed')
    

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('usage: run_sg_scatter.py --module_libdir <modulelibdir> --prepare_outdir <prepare_outdir> --scatter_index <scatter_index> [--cwd  <outputdir>] ')

    print ('python version '+sys.version)
    
    parser = optparse.OptionParser()
    parser.add_option("--scatter_index", dest="scatter_index",
                      help="number 1-N for the given scatter instance")
    parser.add_option("--module_libdir", dest="module_libdir",
                      help="module source code location")
    parser.add_option("--prepare_outdir", dest="prepare_outdir",
                      help="outdir for prepare step")
    parser.add_option("--cwd", dest="cwd",
                      help="working directory for output, defaults to current directory")
    
    (args, args_other) = parser.parse_args()    
    
    
    
    #parser = argparse.ArgumentParser(description='Run one scatter step of a scatter gather module')

    #parser.add_argument('--scatter_index', 
                   #help='number 1-N for the given scatter instance')

    #parser.add_argument('--module_libdir',
                       #help='module source code location')
    #parser.add_argument('--prepare_outdir',
                       #help='outdir for prepare step')
    #parser.add_argument('--cwd',default=None,
                       #help='working directory for output, defaults to current directory')
    
    #args = parser.parse_args()
    
    
    run_scatter(args.module_libdir, args.prepare_outdir, args.scatter_index, args.cwd)