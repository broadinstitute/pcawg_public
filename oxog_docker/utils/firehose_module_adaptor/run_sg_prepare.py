#!/usr/bin/env python
import os
import sys

import common
import run_module


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('usage: run_sg_prepare.py --module_libdir <modulelibdir> [--cwd  <outputdir>] --<prepare module argname> <prepare module argvalue> ...')

    if sys.argv[1] != '--module_libdir':
        print ('first argument must be --module_libdir')
        sys.exit(1)
    module_libdir = sys.argv[2]
    prepare_cmd = common.get_prepare_cmd(module_libdir)
    sg_prepare_template = prepare_cmd + ' ' + module_libdir + '/ ${args} > prepareResults.out'
    args = sys.argv[1:]
    args.append('--sg_prepare_template')
    args.append(sg_prepare_template)
    
    run_module.run_module(args)