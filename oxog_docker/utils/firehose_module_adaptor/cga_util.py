import datetime
import os
import errno
import csv
import hashlib
import sys
import subprocess
import pickle

def get_datestamp():
    t = datetime.datetime.now()
    datestamp = '%d_%02d_%02d' % (t.year, t.month, t.day)
    return datestamp

def get_timestamp(ts = None):
    if ts == None:
        t=datetime.datetime.now()
    else:
        t = datetime.datetime.fromtimestamp(ts)
    timestamp = '%d_%02d_%02d__%02d_%02d_%02d'%(t.year,t.month,t.day,t.hour,t.minute,t.second)
    return timestamp

def get_timestamp_delta(ts_begin,ts_end):        
    begin_secs = timestamp_to_seconds(ts_begin)
    end_secs = timestamp_to_seconds(ts_end)
    duration = end_secs - begin_secs
    return duration

def timestamp_to_seconds(timestamp):
    (year,month,day,junk,hour,minute,second)= timestamp.split('_')
    (year,month,day,junk,hour,minute,second) = (
        int(year),int(month),int(day),junk,int(hour),int(minute),int(second))
    
    dt = datetime.datetime(year,month,day,hour,minute,second)
    days = dt.toordinal()
    days = days - 733000 # keep the seconds to a managable size...
    secs = days*24*60*60 + hour*60*60 + minute*60 + second
    return secs

def safe_make_dirs(dir_name, mode=0o777):
    """Makes directory structure, or ends gracefully if directory already exists"""
    try:
        os.makedirs(dir_name, mode=mode)
    except OSError as value:
        error_num = value.errno
        # what is 183? don't know... came from legacy code.
        if  error_num==errno.EEXIST or error_num==183 :
            pass  # Directory already existed
        else:
            raise  # Reraise other errors
        
def safe_make_symlink(input_file_path,output_file_path):
    output_file_dir = os.path.dirname(output_file_path)
    # Verify the input file is actually there
    if not os.path.exists(input_file_path):
        raise Exception("can't find file %s"%input_file_path)
    safe_make_dirs(output_file_dir)
    try:
        os.symlink(input_file_path,output_file_path)
    except OSError as err:
        if err.errno == errno.EEXIST:
            # link already exists, check that it is identical to the one we are trying to put down
            old = os.path.realpath(input_file_path)
            new = os.path.realpath(output_file_path)
            if old != new:
                raise Exception('Existing file is different than the new symlink')
        else:
            raise
def safe_make_hardlink(input_file_path,output_file_path):
    output_file_dir = os.path.dirname(output_file_path)
    # Verify the input file is actually there
    if not os.path.exists(input_file_path):
        raise Exception("can't find file %s"%input_file_path)
    safe_make_dirs(output_file_dir)
    try:
        os.link(input_file_path,output_file_path)
    except OSError as err:
        if err.errno == errno.EEXIST:
            # link already exists, check that it is identical to the one we are trying to put down
            if not os.path.samefile(input_file_path,output_file_path):
                raise Exception('Existing file is different than the new symlink')
        else:
            raise
                
        
def get_table_header_line(filepath):
    #Handle empty file
    if os.path.getsize(filepath)==0:
        return None
    #open file
    infid = open(filepath)
    inreader = csv.reader(infid,dialect='excel-tab')
    #parse header line
    header_line = inreader.next()
    infid.close()
    return header_line

def write_table_file(file_path,outlines):
    safe_make_dirs(os.path.dirname(file_path))
    outfid = open(file_path,'w')
    outwriter = csv.writer(outfid,dialect='excel-tab',lineterminator='\n')
    outwriter.writerows(outlines)
    outfid.close()
    

def dump_dict_table(filepath,table,fields=None):
    if isinstance(table,dict):
        keys = list(table.keys())
        keys.sort()
        table2 = []
        for key in keys:
            line = table[key]
            table2.append(line)
        table = table2
        
    if table == []:
        fields = []
    elif fields == None:
        fields = table[0].keys()
    fid = open(filepath,'w')
    outdict = csv.DictWriter(fid, dialect='excel-tab', lineterminator = '\n', fieldnames = fields)
    outdict.writerow(dict(zip(fields,fields)))
    outdict.writerows(table)
    fid.close()
    
def read_dict_table(filepath):
    if not os.path.exists(filepath):
        raise Exception('File not found: %s'%filepath)
    if os.path.getsize(filepath) == 0:
        fieldnames = []
        table = []
    else:
        fieldnames = get_table_header_line(filepath)
        infid = open(filepath)
        indict = csv.DictReader(infid,dialect='excel-tab')
        table = []
        for line in indict:
            table.append(line)
        infid.close()
    return (table, fieldnames)
        
    
def write_string_to_file(filename, file_contents):
    """"""
    f=open(filename,'w')
    f.write(file_contents)
    f.close
    
def read_string_from_file(filename,do_rstrip=None):
    ''' 
    Reads contents of filename and returns it as a string.
    If do_rstrip is set to 'rstrip', then also strips any trailing whitespace
    '''
    f=open(filename,'r')
    file_contents = f.read()
    f.close()
    if do_rstrip == 'rstrip':
        file_contents = file_contents.rstrip()
    return file_contents

def pickle_read_object(picklepath):
    picklepath_tmp = picklepath + '.tmp'    
    if os.path.exists(picklepath):
        fpath = picklepath
    elif os.path.exists(picklepath_tmp):
        fpath = picklepath_tmp
    else:
        fpath = None
        #raise Exception('Could not find %s'%picklepath)
        
    if fpath != None and os.path.getsize(fpath) > 0:
        fid = open(fpath, 'rb')
        obj = pickle.load(fid)
    else:
        obj = None
    return obj

    
def pickle_dump_object(obj, picklepath):
    file_dir = os.path.dirname(picklepath)
    if not os.path.exists(file_dir):
        safe_make_dirs(file_dir)
    if not os.path.exists(picklepath):
        fid = open(picklepath, 'w')
        fid.close()

    picklepath_tmp = picklepath + '.tmp'
    fid = open(picklepath_tmp, 'wb')
    try:
        pickle.dump(obj, fid, -1)
    except:
        fid.close()
        os.remove(picklepath_tmp)
        raise
    fid.close()
    if os.path.exists(picklepath):
        os.remove(picklepath) # This remove step is the atomic commit.
    os.rename(picklepath_tmp, picklepath)

    
def compute_md5(infile):
    cmd = 'md5sum ' + infile
    (err,stdout_str,stderr_str) = execute_str(cmd)
    if not err:
        md5 = stdout_str[:32]
    else:
        md5 = None
    return md5
    
def compute_md5_old(infile):
    blocksize=pow(2,20) #1MB 
    infid = open(infile,'r')
    hasher = hashlib.md5()
    block = infid.read(blocksize)
    while len(block)>0:
        hasher.update(block)
        block = infid.read(blocksize)
    md5 = hasher.hexdigest()
    infid.close()
    return md5
    
def execute_str(cmd_str):

    p = subprocess.Popen(cmd_str,shell=True,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
    (stdout,stderr) = p.communicate()
    return_code = p.returncode
    err = return_code!=0
    stdout_str = str(stdout)
    stderr_str = str(stderr)

    return (err,stdout_str,stderr_str)


def find_dcc_files(dcc_mirror_dir,dir_depth,dir_depth_back,dir_pattern,file_extension):
    #11,1, 'mage-tab', '.sdrf.txt'
    sdrf_paths = ['/xchip/gdac_data/dcc_mirror3/dcc_site/tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/ov/cgcc/broad.mit.edu/ht_hg-u133a/transcriptome/broad.mit.edu_OV.HT_HG-U133A.mage-tab.1.1002.0/broad.mit.edu_OV.HT_HG-U133A.sdrf.txt']
    #11,2 'tracerel', '.maf'
    #maf_paths = ['/xchip/gdac_data/dcc_mirror3/dcc_site/tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/tcga4yeo/tumor/gbm/gsc/broad.mit.edu/abi/tracerel/broad.mit.edu_GBM.ABI.1.32.0/broad.mit.edu_GBM.ABI.1.maf']
    #test_filepath = os.path.join('/xchip/cga1/rui/test/','t1.txt')    
    if True:
        sdrf_extension = file_extension
        len_sdrf_extension = len(sdrf_extension)
        sdrf_paths = []
        dcc_mirror_dir_list = dcc_mirror_dir.split('/')
        if dcc_mirror_dir_list[-1] != 'tcga-data.nci.nih.gov':
            raise Exception('dcc_mirror_dir must be path ending with "tcga-data.nci.nih.gov"')
        for filedir,subdir_list,file_list in os.walk(dcc_mirror_dir):
            filedir_list = filedir.split('/')
            # Expect sdrf files only at the appropriate directory depth.
            if len(filedir_list)-len(dcc_mirror_dir_list) != dir_depth:
                continue
            if not dir_pattern in filedir_list[-dir_depth_back]:
                continue
            #if test_short and 'hudsonalpha' in filedir_list[-1]:
            #    continue
            #if not ('usc' in filedir_list[-1] or 'hudson' in filedir_list[-1]):
                #continue
            for filename in file_list:
                if len (filename) > len_sdrf_extension and filename[-len_sdrf_extension:] == sdrf_extension:
                    
                    sdrf_path = os.path.join(filedir,filename)
                    sdrf_paths.append(sdrf_path)
                    #don't break, there may be multiple sdrfs in the directory...
                                        
    return sdrf_paths

def find_file_in_path(filename,pathlist=None):
    if pathlist==None:
        syspath = sys.path
    else:
        syspath = pathlist
    for dirname in syspath:
        if dirname == '':
            dirname = os.getcwd()
        filepath = os.path.join(dirname,filename)
        if os.path.exists(filepath):
            break
    else:
        raise Exception ('Could not find file %s in seach path'%filename)
    return filepath