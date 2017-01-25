__author__ = 'gsaksena'
import argparse
import subprocess
import os

def execute_str(cmd_str):
    print(cmd_str)
    p = subprocess.Popen(cmd_str,shell=True,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
    (stdout,stderr) = p.communicate()
    return_code = p.returncode
    err = return_code!=0
    stdout_str = stdout.decode('ascii')
    stderr_str = stderr.decode('ascii')

    return (err,stdout_str,stderr_str)

def create_tabix_files(libdir, cwd, input_file, output_base_name, output_extension):
    
    initial_filename = '{0}{1}'.format(output_base_name,output_extension)
    gz_filename = '{0}.gz'.format(initial_filename)
    gz_tbi_filename = '{0}.tbi'.format(gz_filename)

    initial_path = os.path.join(cwd,initial_filename)
    gz_path = os.path.join(cwd, gz_filename)
    gz_tbi_path = os.path.join(cwd, gz_tbi_filename)

    cp_cmdstr = 'cp {0} {1}'.format(input_file, initial_path)
    (err,stdout_str,stderr_str) = execute_str(cp_cmdstr)
    if err:
        raise Exception(stderr_str)


    bgzip_cmdstr = '{0}/bgzip {1}'.format(libdir, initial_path)
    (err,stdout_str,stderr_str) = execute_str(bgzip_cmdstr)
    if err:
        raise Exception(stderr_str)

    tabix_cmdstr = '{0}/tabix {1}'.format(libdir, gz_path)
    (err,stdout_str,stderr_str) = execute_str(tabix_cmdstr)
    if err:
        raise Exception(stderr_str)



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate tabix .gz and .gz.tbi files. Outputs files named <base_name><extension>.gz and <base_name><extension>.gz.tbi files.')
    parser.add_argument('--libdir',help='')
    parser.add_argument('--input_file',help='input path, eg a .vcf file')
    parser.add_argument('--output_base_name',help='eg ffab49f1-9fa6-4879-abab-6c51ca1ce948')
    parser.add_argument('--output_extension',help='eg .snv_mnv.vcf')
    args = parser.parse_args()
    #copy the args into global namespace...
    globals().update(args.__dict__)
    cwd = os.getcwd()
    create_tabix_files(libdir, cwd, input_file, output_base_name, output_extension)

