#!/usr/bin/env python

import sys
import common
import os
import optparse
import getopt
import datetime
import getpass
import subprocess

import cga_util

def run_module(argv):
    (module_name,lsid,cache_path,execution_id,module_libdir,cwd,sg_prepare_template) = parse_argv_module_id(argv)
    if module_libdir is not None:
        cached_module_dir = module_libdir
    else:
        cached_module_dir = common.get_cached_module_dir(module_name,lsid,cache_path)
    if not os.path.exists(cached_module_dir):
        raise Exception('Could not find module %s lsid %s'%(module_name,lsid))
    manifest_path = os.path.join(cached_module_dir,'manifest')
    manifest_str = cga_util.read_string_from_file(manifest_path,'no_rstrip')
    (module_arg_names,arg_prefix_dict)=common.get_module_arg_names(manifest_str)
    (arg_param_value_dict,missing_args) = parse_argv_module_args(argv,module_arg_names)
    validate_path_args(arg_param_value_dict)
    exe_template_str = common.get_module_exe_line(manifest_str)
    executor = common.get_executor(manifest_str)
    (exe_str,error_mode) = build_exe_str(exe_template_str,arg_param_value_dict,missing_args,arg_prefix_dict,cached_module_dir,executor)
    if sg_prepare_template is not None and len(sg_prepare_template)>0:
        exe_str = sg_prepare_template.replace('${args}',exe_str)
        
    logparams = (module_arg_names,arg_param_value_dict,module_name,lsid,execution_id)
    passing = execute_command_like_gp(exe_str, cwd, logparams,error_mode)
    
    if passing:
        sys.exit(0)
    else:
        sys.exit(1)
    
    
def generate_gp_execution_log(logparams,exe_str):
# Created: Wed Dec 01 23:13:08 EST 2010 by cgaadm_deadline
# Job: 123456    server:  http://cga-genepattern:8090/gp/
# Module: TsvToList urn:lsid:broadinstitute.org:cancer.genome.analysis:00298:8
# Parameters: 
#    tsv.file = abc.tsv
#    picard.extension = 
    
    (module_arg_names,arg_param_value_dict,module_name,lsid,execution_id) = logparams
    out_list = []
        
    t=datetime.datetime.now()
    timestamp = t.ctime()
    try:
        user = getpass.getuser()
    except KeyError:
        user = "unknown"
    line = '# Created: %s by %s'%(timestamp,user)
    out_list.append(line)
    
    line = '# Job: %-10sserver:  %s'%(execution_id, 'none')
    out_list.append(line)
    
    line = '# Module: %s %s'%(module_name,lsid)
    out_list.append(line)

    line = '# Parameters: '
    out_list.append(line)

    for arg_name in module_arg_names:
        arg_value = arg_param_value_dict.get(arg_name,'')
        line = '#    %s = %s'%(arg_name,arg_value)
        out_list.append(line)
        
    line = '# '
    out_list.append(line)
    
    line = exe_str
    out_list.append(line)

    gp_execution_log_str = '\n'.join(out_list) + '\n'

    return gp_execution_log_str



def execute_command_like_gp(exe_str, cwd, logparams, error_mode):
    
    if os.path.exists(cwd):
        #raise Exception ('Output directory already exists: %s'%cwd)
        pass
    
    stdout_path = os.path.join(cwd,'stdout.txt')
    stderr_path = os.path.join(cwd,'stderr.txt')
    gp_execution_log_path = os.path.join(cwd,'gp_execution_log.txt')
    
    cga_util.safe_make_dirs(cwd)
    
    if logparams is not None:
        gp_execution_log_str = generate_gp_execution_log(logparams, exe_str)
    else:
        gp_execution_log_str = exe_str + '\n'
    
    cga_util.write_string_to_file(gp_execution_log_path,gp_execution_log_str)
    stdout_fid = open(stdout_path,'w')
    stderr_fid = open(stderr_path,'w')

    
    exit_code = subprocess.call(exe_str,shell=True,stdout = stdout_fid,stderr = stderr_fid,cwd = cwd)
    
    stdout_fid.close()
    stderr_fid.close()
    
    if not os.path.exists(stderr_path) or os.path.getsize(stderr_path)==0:
        stderr_empty = True
    else:
        stderr_empty = False
    
    if error_mode == 'exit_code':
        passing = (exit_code == 0)
    elif error_mode == 'stderr':
        passing = stderr_empty
    else:
        raise Exception ('unrecognized value for error_mode')
        
    return passing

    
    
    
def build_exe_str(exe_template_str,arg_param_value_dict,missing_args,arg_prefix_dict,cached_module_dir,executor):
    param_value_dict = arg_param_value_dict
    param_value_dict.update(common.special_params)
    param_value_dict['libdir']=cached_module_dir + '/'

    for field in missing_args:
        param_value_dict[field]=''
    
    exe_list = exe_template_str.split()
    if exe_list[0]=='<bsub>':
        exe_list = exe_list[3:]
        error_mode = 'exit_code'
    elif exe_list[0]=='<matlab-2009a>':
        exe_list = exe_list[1:]
        error_mode = 'exit_code'
    elif executor == 'ScatterGather':
        error_mode = 'exit_code'
    else:
        #error_mode = 'stderr'
        #presumably no more gp modules remain in FH?
        error_mode = 'exit_code'
    exe_str = ' '.join(exe_list)
    
    params = param_value_dict.keys()
    for param in params:
        param_str = '<%s>'%param
        val_str = param_value_dict[param]
        arg_prefix = arg_prefix_dict.get(param,'')
        if len(val_str)>0 and len(arg_prefix)>0:
            val_str = arg_prefix + ' ' + val_str
        exe_str = exe_str.replace(param_str,val_str)
    
    return (exe_str,error_mode)

def validate_path_args(arg_param_value_dict):
    for param in arg_param_value_dict:
        val = arg_param_value_dict[param]
        if len(val)<2 or val[0]!='/':
            continue
        if not os.path.exists(val):
            raise Exception('File not found: param %s val %s'%(param,val))
    
def parse_argv_module_args(argv,module_arg_names):
    baseline_args = ['module_name','lsid','cache_path','execution_id','cwd','module_libdir','sg_prepare_template']
    arg_names = baseline_args + module_arg_names
    arg_names2 = [argname + '=' for argname in arg_names]
    (param_value_pairs,remnants)=getopt.getopt(argv,"",arg_names2)
    if len(remnants)>0:
        raise Exception ("unrecognized arguments %s"%str(remnants))
    arg_param_value_dict = {}
    missing_args = set(module_arg_names)
    for arg_pair in param_value_pairs:
        param_name = arg_pair[0][2:] #strip off '--' at start
        param_value = arg_pair[1]
        if param_name not in module_arg_names:
            continue
        missing_args.remove(param_name)
        if param_name in arg_param_value_dict:
            raise Exception('duplicated param name %s'%param_name)
        arg_param_value_dict[param_name]=param_value
    missing_args = list(missing_args)
        
    return (arg_param_value_dict,missing_args)
    
def parse_argv_module_id(argv):    
    #baseline_args = ['module_name','lsid','cache_path','execution_id','cwd']
    arg_names = [arg_name[2:]+'=' for arg_name in argv if arg_name[:2]=='--']
    
    (param_value_pairs,remnants)=getopt.gnu_getopt(argv,"",arg_names)
    
    #Set defaults
    module_name = ''
    lsid = ''
    cache_path = common.cache_path
    execution_id = '0'
    module_libdir = None
    cwd = os.getcwd()
    sg_prepare_template = ''
    
    args_used = set()
    for arg_pair in param_value_pairs:
        param_name = arg_pair[0][2:] #strip off '--' at start
        param_value = arg_pair[1]
        if param_name in args_used:
            raise Exception ('duplicated parameter: %s'%param_name)
        else:
            args_used.add(param_name)
        if param_name == 'module_name': 
            module_name = param_value
        elif param_name == 'lsid':
            lsid = param_value
        elif param_name == 'cache_path':
            cache_path = param_value
        elif param_name == 'execution_id':
            execution_id = param_value
        elif param_name == 'sg_prepare_template':
            sg_prepare_template = param_value
        elif param_name == 'module_libdir':
            module_libdir = param_value
            module_name = os.path.basename(module_libdir)
        elif param_name == 'cwd':
            cwd = param_value
            
    
    # test harness
    #module_name = "CancerBirdseedSNPsToGeli"
    #lsid = "urn_lsid_broadinstitute_org_cancer_genome_analysis_00038_12"
    #cache_path = common.cache_path
    #execution_id = "0"
    #cwd = "/xchip/tcga_scratch/gsaksena/test_run_gp_module"
    
    return (module_name,lsid,cache_path,execution_id,module_libdir,cwd,sg_prepare_template)
    
if __name__=='__main__':
    
    argv = sys.argv[1:]
    run_module(argv)
    
 

