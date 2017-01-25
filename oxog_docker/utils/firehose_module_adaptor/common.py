import os
import sys
import string
import xml.parsers.expat

zip_search_path = ['/xchip/tcga_scratch/gsaksena/CancerGenomeAnalysis/trunk/analysis_pipeline/genepattern/build']
cache_path='/xchip/tcga_scratch/gsaksena/gp_cache'

special_params = {
    'java':'source /broad/tools/scripts/useuse; reuse -q Java-1.6; java',
    'R':'source /broad/tools/scripts/useuse; reuse -q R-2.11; R',
    'matlab':'source /broad/tools/scripts/useuse; reuse -q Matlab-2009a; matlab',
    'tmp.dir':'/broad/shptmp/gsaksena',
    }




def thunk(input_str):
    ok_chars = string.letters + string.digits + "_"
    output_str = input_str
    for character in output_str:
        if not character in ok_chars:
            output_str = output_str.replace(character,"_")
    if output_str[0] in string.digits:
        output_str = "_" + output_str
    return output_str    

def get_cached_module_dir(module_name,lsid,cache_path):
    module_name_thunked = thunk(module_name)
    lsid_thunked = thunk(lsid)
    cached_module_dir = os.path.join(cache_path,module_name_thunked,lsid_thunked)
    return cached_module_dir

def get_lsid_from_manifest(manifest_str):
    lsid_line = _get_field_from_manifest("LSID",manifest_str)
    if lsid_line == None:
        raise Exception("could not find field %s line in manifest"%"LSID")
    lsid = thunk(lsid_line)
    return lsid
    
def _get_field_from_manifest(fieldname,manifest_str):
    lines = manifest_str.splitlines()
    for line in lines:
        if line.startswith(fieldname):
            value_line = line[len(fieldname)+1:]
            value = value_line.replace('\\','')
            break
    else:
        value = None
        #raise Exception("could not find field %s line in manifest"%fieldname)
    return value
    
def get_module_arg_names(manifest_str):
    argnum=0
    arg_names = []
    arg_prefix_dict = {}
    while True:
        argnum = argnum + 1
        argname_fieldname = "p%d_name"%argnum
        argname_value = _get_field_from_manifest(argname_fieldname,manifest_str)
        if argname_value == None:
            break
        else:
            arg_names.append(argname_value)
            arg_prefix_fieldname = "p%d_prefix_when_specified"%argnum
            arg_prefix_value = _get_field_from_manifest(arg_prefix_fieldname,manifest_str)
            if arg_prefix_value == None:
                arg_prefix_value = ''
            arg_prefix_dict[argname_value]=arg_prefix_value
    return (arg_names, arg_prefix_dict)

def get_executor(manifest_str):
    value = _get_field_from_manifest('executor',manifest_str)
    return value

def get_module_exe_line(manifest_str):
    exe_line = _get_field_from_manifest("commandLine",manifest_str)
    if exe_line == None:
        raise Exception("could not find field %s line in manifest"%"commandLine")
    return exe_line


prepareExe = None
prepareMemory = None
scatterExe = None
scatterMemory = None
scatterMatlab = False
scatterMatlabDisplay = False
scatterMatlabJavaMemory = None
maxScatterTries = "3"
gatherExe = None
gatherMemory = None
gatherMatlab = False
gatherMatlabJavaMemory = None

dryRun = False
runPrepareLocal = False

lsfProject = None

def start_element(name, attrs):
    if name == "scatter-gather":
        global dryRun
        dryRun = (attrs.get("dry-run") == "true")
        global lsfProject
        lsfProject = attrs.get("lsf-project")
    if name == "prepare":
        global prepareExe
        prepareExe = attrs["exe"]
        global prepareMemory
        prepareMemory = attrs.get("memory")
        global runPrepareLocal
        runPrepareLocal = (attrs.get("run-local") == "true")
    elif name == "scatter":
        global scatterExe
        scatterExe = attrs["exe"]
        global scatterMemory
        scatterMemory = attrs["memory"]
        global scatterMatlab
        scatterMatlab = (attrs.get("matlab") == "true")
        global scatterMatlabDisplay
        scatterMatlabDisplay = (attrs.get("matlabDisplay") == "true")
        global scatterMatlabJavaMemory
        scatterMatlabJavaMemory = attrs.get("matlabJavaMemory")
        global maxScatterTries
        if attrs.get("maxTries") is not None:
            maxScatterTries = attrs.get("maxTries")
    elif name == "gather":
        global gatherExe
        gatherExe = attrs["exe"]
        global gatherMemory
        gatherMemory = attrs.get("memory")
        global gatherMatlab
        gatherMatlab = (attrs.get("matlab") == "true")
        global gatherMatlabJavaMemory
        gatherMatlabJavaMemory = attrs.get("matlabJavaMemory")
        

def parse_sg_configfile(libdir):
    scatter_gather_configfile = os.path.join(libdir,'scatter-gather.xml')
    commandParser = xml.parsers.expat.ParserCreate()
    commandParser.StartElementHandler = start_element
    commandFile = open(scatter_gather_configfile,'rb')
    commandParser.ParseFile(commandFile)
    commandFile.close()
        
def get_prepare_cmd(libdir):        
    parse_sg_configfile(libdir)
    prepareExe2 = prepareExe
    prepareExe2 = prepareExe2.replace("${libdir}", libdir+'/')
    return prepareExe2

def get_scatter_cmd(libdir):
    parse_sg_configfile(libdir)
    scatterExe2 = scatterExe
    scatterExe2 = scatterExe2.replace("${libdir}", libdir+'/')
    return scatterExe2

def get_gather_cmd(libdir):
    parse_sg_configfile(libdir)
    gatherExe2 = gatherExe
    gatherExe2 = gatherExe2.replace("${libdir}", libdir+'/')
    return gatherExe2
    
