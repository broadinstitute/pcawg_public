#!/usr/bin/env python

import sys
import common
import os
import optparse
import zipfile


# Add the trunk/Python/util directory to the PYTHONPATH
scr = sys.argv[0]
scr = os.path.abspath(scr)
scr_list = scr.split('/')
trunk_pos = scr_list.index('trunk')
util_path = '/'.join(scr_list[:trunk_pos+1] + ['Python','util'])

sys.path.append(util_path)
import cga_util



def register_module(module_name,zip_search_path,cache_path):

    #Find and open the input zip file
    zip_input_filename = module_name + ".zip"
    zip_input_filepath = cga_util.find_file_in_path(zip_input_filename,zip_search_path)
    
    zip_in = zipfile.ZipFile(zip_input_filepath)
    #Get the LSID from the input zip file
    manifest_contents = zip_in.read("manifest")
    lsid = common.get_lsid_from_manifest(manifest_contents)
    
    #Prepare output dir
    cached_module_dir = common.get_cached_module_dir(module_name,lsid,cache_path)
    # TODO - create filesystem-based semaphore
    if os.path.exists(cached_module_dir):
        raise Exception('Already registered module %s lsid %s'%(module_name,lsid))
    cga_util.safe_make_dirs(cached_module_dir)
    
    #files are stored in flat directory structure within zip.
    #unzip them into the output dir
    zip_filelist = zip_in.namelist()
    for component_filename in zip_filelist:
        file_contents = zip_in.read(component_filename)
        out_filepath = os.path.join(cached_module_dir,component_filename)
        cga_util.write_string_to_file(out_filepath,file_contents)
    zip_in.close()
    
    #All done.
    print ('Successfully registered module %s lsid %s'%(module_name,lsid))


if __name__=='__main__':

    parser = optparse.OptionParser()
    parser.add_option( "--module_name", dest="module_name",help="")
    #parser.add_option( "--lsid", dest="lsid",help="")
    parser.add_option( "--zip_search_path", dest="zip_search_path",
                       default=common.zip_search_path,action="append",help="")
    parser.add_option( "--cache_path", dest="cache_path",default=common.cache_path,
                       help="")


    (options, args) = parser.parse_args()
    
    module_name = options.module_name
    #lsid = options.lsid
    zip_search_path = options.zip_search_path
    cache_path = options.cache_path
    
    register_module(module_name,zip_search_path,cache_path)