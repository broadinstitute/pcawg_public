#!/broad/software/free/Linux/redhat_5_x86_64/pkgs/python_2.6.5/bin/python

import sys
import subprocess
import os
import random
import xml.parsers.expat
import glob

RETRY_RETURN_CODE = 167

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
        prepareExe = insertLibdir(attrs["exe"]).split()
        global prepareMemory
        prepareMemory = attrs.get("memory")
        global runPrepareLocal
        runPrepareLocal = (attrs.get("run-local") == "true")
    elif name == "scatter":
        global scatterExe
        scatterExe = insertLibdir(attrs["exe"])
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
        gatherExe = insertLibdir(attrs["exe"])
        global gatherMemory
        gatherMemory = attrs.get("memory")
        global gatherMatlab
        gatherMatlab = (attrs.get("matlab") == "true")
        global gatherMatlabJavaMemory
        gatherMatlabJavaMemory = attrs.get("matlabJavaMemory")

def printUsage():
    print "Usage: scatter-gather.py [options] <lsf queue> <scatter-gather.xml file> <libdir> program parameters"
    print "Also see the documentation at http://iwww.broadinstitute.org/cancer/cga/wiki/index.php/Scatter-Gather"
    print "options:"
    print "    --job-suffix a suffix that must be unique among jobs that lsf knows about"
    print "    --priority what priority to assign scatter and gather jobs (-sp bsub flag)"

def addMatlabPrefix(command):
    global libdir
    global scatterMatlabDisplay
    matlabCommandPrefix = ["python", libdir + "run_matlab.py"]
    if scatterMatlabDisplay:
        matlabCommandPrefix.append("--with-display")
    matlabCommandPrefix.append(libdir)
    return " ".join(matlabCommandPrefix) + " "  + command

def insertLibdir(command):
    global libdir
    return command.replace("${libdir}", libdir)

def parseArguments(argumentLine):
    parts = argumentLine.split()
    arguments = [];
    quote = None
    for part in parts:
        if quote is not None:
            if part.endswith(quote):
                currentArgument = " ".join([currentArgument, part.rstrip(quote)])
                arguments.append(currentArgument)
                quote = None
            else:
                currentArgument = " ".join([currentArgument, part])
        elif part[0] in "\"'":
            quote = part[0]
            if part.endswith(quote):
                arguments.append(part.strip(quote))
                quote = None
            else:
                currentArgument = part.lstrip(quote)
        else:
            arguments.append(part)
    return arguments 
    
#generates a shell script to feed to LSF
#we could put this in another file but don't want to have to distribute another file
def generateScatterScript_old(command, arguments, triesFileName, maxTries):
    script = []
    script.append("tries=`cat %s`" % triesFileName)
    script.append("echo -n `expr ${tries} + 1` > %s" % triesFileName)
    script.append(command + " " + arguments)
    script.append("return_code=$?")
    script.append("if [ ${return_code} -ne 0 ]; then")
    script.append("tries=`cat %s`" % triesFileName)
    script.append("if [ ${return_code} -ne 130 -a ${tries} -lt %s ]; then" % maxTries) # 130 is the LSF exit code for user terminated
    script.append("exit %d;" % RETRY_RETURN_CODE)
    script.append("else")
    script.append("echo ${return_code} > exited;")
    script.append("exit 1;")
    script.append("fi")
    script.append("else")
    script.append("touch success")
    script.append("fi")
    return "\n".join(script);


def execute_command(cmd_str, cwd, outfn, errfn):

    try:
        #os.makedirs(cwd)
        pass
    except:
        pass
    os.chdir(cwd)
    outfid = open(os.path.join(cwd,outfn),'w+')
    errfid = open(os.path.join(cwd,errfn),'w+')
    cmdfid = open(os.path.join(cwd,'cmdstr.txt'),'w')
    cmdfid.write(' '.join(cmd_str))
    cmdfid.close()
    err = subprocess.call(cmd_str, shell=True, stdout=outfid, stderr=errfid)# , cwd=cwd)
    if err != 0:
        outfid.seek(0)
        errfid.seek(0)
        out_str = outfid.read()
        err_str = errfid.read()
        print "error code %d"%err
        print "\nstdout"
        print out_str
        print "\nstderr"
        print err_str
    return err



def executeScatterScript(command, arguments, cwd):

        cmd_str = command + ' ' + arguments
        outfn = 'scatter.out'
        errfn = 'scatter.err'
        err = execute_command(cmd_str, cwd, outfn, errfn)

        if err == 0:
            fid=open(os.path.join(cwd,'success'),'w')
        else:
            fid = open(os.path.join(cwd,'exited'),'w')
            fid.write("%d\n"%err)
            #raise exception?
        fid.close()



#generates a shell script to feed to LSF
#we could put this in another file but don't want to have to distribute another file
def executeGatherScript(command, arguments, jobCount, cwd):
    exited_paths = glob.glob(os.path.join(cwd,'scatter.*/exited'))
    if len(exited_paths)>0:
        raise Exception('some scatter jobs failed')

    success_paths = glob.glob(os.path.join(cwd,'scatter.*/success'))
    if len(success_paths) != jobCount:
        raise Exception('expected result files missing, try rerunning')

    cmd_str = command + ' ' + arguments
    err = execute_command(cmd_str, cwd, 'gather.out', 'gather.err')

    if err!=0:
        raise Exception('gather failed')


    # #clean up
    # script.append("rm -f prepareResults.out")
    # catAndRm("'*** Prepare stdandard out:'", "prepare.out", script)
    # catAndRm("'*** Prepare stdandard err:'", "prepare.err", script)
    # catAndRm("'*** Scatter stdandard out:'", "scatter.*/scatter.out", script)
    # catAndRm("'*** Scatter stdandard err:'", "scatter.*/scatter.err", script)
    # script.append("echo '*** Scatter retry log:'")
    # script.append("head scatter.*.tries")
    # script.append("echo") # new line
    # script.append("rm -f scatter.*.tries")
    #
    # script.append("rm -f scatter.*/success")
    # if scatterMatlabJavaMemory is not None:
    #     script.append("rm -f scatter.*/java.opts")
    # script.append("rmdir scatter.*")
    #
    # return "\n".join(script);

def generateGatherScript_old(command, arguments, jobCount):
    script = []
    script.append("ls scatter.*/exited 2>&1")
    script.append("return_code=$?")
    script.append("if [ ${return_code} -eq 0 ]; then")
    script.append("echo some scatter jobs failed;")
    script.append("exit 1;")
    script.append("fi")
    script.append("if [ `ls -1 scatter.*/success | wc -l` -ne %d ]; then" % (jobCount))
    script.append("echo expected result files missing, try rerunning;")
    script.append("exit 1;")
    script.append("fi")
    script.append(command + " " + arguments)
    script.append("return_code=$?")
    script.append("if [ ${return_code} -ne 0 ]; then")
    script.append("echo gather failed;")
    script.append("exit ${return_code};")
    script.append("fi")

    #clean up
    script.append("rm -f prepareResults.out")
    catAndRm("'*** Prepare stdandard out:'", "prepare.out", script)
    catAndRm("'*** Prepare stdandard err:'", "prepare.err", script)
    catAndRm("'*** Scatter stdandard out:'", "scatter.*/scatter.out", script)
    catAndRm("'*** Scatter stdandard err:'", "scatter.*/scatter.err", script)
    script.append("echo '*** Scatter retry log:'")
    script.append("head scatter.*.tries")
    script.append("echo") # new line
    script.append("rm -f scatter.*.tries")

    script.append("rm -f scatter.*/success")
    if scatterMatlabJavaMemory is not None:
        script.append("rm -f scatter.*/java.opts")
    script.append("rmdir scatter.*")

    return "\n".join(script);

def catAndRm(message, path, script):
    script.append("echo " + message + " 2>&1")
    script.append("cat " + path + " 2>&1")
    script.append("rm -f " + path + " 2>&1")

# constructs a commandline script to be fed to an lsf pre-exec command (-E)
# that will ensure that all required directories are mounted    
def lsfPreExecRequiredDirectories(libdir, cwd, argumentList):
    preExecParts = ['cd "%s"' % (getFileSystem(argument)) 
                    for argument in argumentList 
                    if os.path.exists(argument)]
    #resolve links
    preExecParts = preExecParts + ['cd "%s"' % (getFileSystem(os.path.realpath(argument))) 
                    for argument in argumentList 
                    if os.path.islink(argument)]

    preExecParts = preExecParts + ['cd "%s"' % (getFileSystem(dir)) for dir in [libdir,cwd]]
    
    return " && ".join(set(preExecParts))

def getFileSystem(path):
    if not os.path.exists(path):
        raise Exception("path does not exist: " + path)
    currentPath = os.path.abspath(path)
    while not os.path.ismount(currentPath):
        currentPath = os.path.dirname(currentPath)
    return currentPath

if len(sys.argv) == 1:
    printUsage()
    sys.exit(1)

argOffset = 0
jobSuffix = "%d" % (random.randrange(0,100000))
priority = None
while True:
    if len(sys.argv) <= argOffset + 2:
        break;
    if sys.argv[1+argOffset] == "--job-suffix":
        jobSuffix = sys.argv[2+argOffset]
    elif sys.argv[1+argOffset] == "--priority":
        priority = sys.argv[2+argOffset]
    else:
        break;
    argOffset += 2

NUM_ARGS = 4 + argOffset

if len(sys.argv) < NUM_ARGS:
    printUsage()
    sys.exit(1)
    
queue = sys.argv[1 + argOffset]
libdir = sys.argv[3 + argOffset]

os.environ["PATH"] = ".:" + os.environ["PATH"]

commandParser = xml.parsers.expat.ParserCreate()
commandParser.StartElementHandler = start_element
commandFile = open(sys.argv[2 + argOffset])
commandParser.ParseFile(commandFile)
commandFile.close()

if "SG_DRY_RUN" in os.environ.keys():
    dryRun = (os.environ["SG_DRY_RUN"] == "true")
if "SG_LSF_PROJECT" in os.environ.keys():
    lsfProject = os.environ["SG_LSF_PROJECT"]

if prepareExe is None or scatterExe is None or scatterMemory is None or gatherExe is None:
    printUsage()
    sys.exit(1)


bsubCmd = ["bsub", 
           "-r", 
           #"-mig", "6",
           "-q", queue,
           "-E", lsfPreExecRequiredDirectories(libdir, os.getcwd(), sys.argv[NUM_ARGS:]),
           "-e", "prepare.err", 
           "-oo", "prepare.out",
           "-R", "select[tmp>1000 && scratch>1]",
           "-K"]
if prepareMemory is not None:
    bsubCmd = bsubCmd + ["-R", "rusage[mem=%s]" % (prepareMemory)]
if lsfProject is not None:
    bsubCmd = bsubCmd + ["-P", lsfProject]
    
prepareCmd = prepareExe + [libdir] + sys.argv[NUM_ARGS:]
cmd_str = ' '.join(prepareCmd)

print "calling: ", " ".join( prepareCmd)
base_wd = os.getcwd()
execute_command(cmd_str, base_wd, "prepareResults.out", "prepareResults.err")
# if True or dryRun or runPrepareLocal:
#     prepareOut = open("prepareResults.out", "w");
#     subprocess.check_call(prepareCmd, stdout=prepareOut)
#     prepareOut.close()
# else:
#     subprocess.check_call(bsubCmd + prepareCmd + [">", "prepareResults.out"], stderr=subprocess.STDOUT)
    
prepareOutputFile = open("prepareResults.out")
scatterArgumentsList = prepareOutputFile.readlines()
gatherArguments = scatterArgumentsList.pop()

jobSubmissionInfoFile = open("jobSubmissions.txt", "w")
try:
    bsubCmdBase = ["bsub", 
                   "-r", 
                   #"-mig", "6",
                   "-R", "rusage[mem=%s]" % (scatterMemory), 
                   "-R", "select[tmp>1000 && scratch>1]",
                   "-q", queue]
    if lsfProject is not None:
        bsubCmdBase = bsubCmdBase + ["-P", lsfProject]
    if priority is not None:
        bsubCmdBase = bsubCmdBase + ["-sp", priority]
    
    if scatterMatlab:
        scatterExe = addMatlabPrefix(scatterExe)
    jobNumber = 0
    for scatterArguments in scatterArgumentsList:
        jobNumber = jobNumber+1
        if os.path.exists(os.path.join(base_wd,"scatter.%010d" % (jobNumber), "success")):
            continue
        if not os.path.exists("scatter.%010d" % (jobNumber)):
            os.mkdir(os.path.join(base_wd,"scatter.%010d" % (jobNumber)))
        
        triesFileName = os.path.join(base_wd, "scatter.%010d.tries" % (jobNumber))
        triesFile = open(triesFileName, "w")
        triesFile.write("0")
        triesFile.close();
        
        cwd = os.path.join(base_wd, "scatter.%010d" % (jobNumber))
        preexec = 'true' # "rm -rf %s/*" % (cwd)
        if scatterMatlabJavaMemory is not None:
            preexec += "; echo -Xmx%sg > %s/java.opts" % (scatterMatlabJavaMemory,cwd)
        #preexec += "; " + lsfPreExecRequiredDirectories(libdir, cwd, parseArguments(scatterArguments))
        bsubCmd = bsubCmdBase + ["-cwd", cwd,
                                 "-E", preexec,
                                 "-Q", "EXCLUDE(%d)" % RETRY_RETURN_CODE,
                                 "-e", "scatter.err", 
                                 "-o", "scatter.out",
                                 "-J", "scatter_%s" % (jobSuffix)]
        print "calling: ", scatterExe, " ", scatterArguments

        if not dryRun:
            executeScatterScript(preexec + '; ' + scatterExe, scatterArguments, cwd)
        # if not dryRun:
        #     process = subprocess.Popen(bsubCmd, stdout=jobSubmissionInfoFile, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)
        #     process.communicate(generateScatterScript(scatterExe, scatterArguments, triesFileName, maxScatterTries))
        #     if process.returncode != 0:
        #         raise Exception("Non-zero return code dispatching to LSF")
    
    preexec = "true"
    if gatherMatlabJavaMemory is not None:
        preexec += "echo -Xmx%sg > %s/java.opts; " % (gatherMatlabJavaMemory,os.getcwd())
    #preexec += lsfPreExecRequiredDirectories(libdir, os.getcwd(), parseArguments(gatherArguments))
        
    bsubCmd = ["bsub", 
               "-E", preexec, 
               "-q", queue,
               "-R", "select[tmp>1000 && scratch>1]",
               "-e", "scatter-gather.err.txt", 
               "-o", "scatter-gather.out.txt",
               "-w", "ended(scatter_%s)" % (jobSuffix),
               "-J", "gather_%s" % (jobSuffix)]
    if lsfProject is not None:
        bsubCmd = bsubCmd + ["-P", lsfProject]
    if gatherMemory is not None:
        bsubCmd = bsubCmd + ["-R", "rusage[mem=%s]" % (gatherMemory)]
    if priority is not None:
        bsubCmd = bsubCmd + ["-sp", priority]
    
    if gatherMatlab:
        gatherExe = addMatlabPrefix(gatherExe)
        
    print "calling: ", " ".join(bsubCmd), " ", gatherExe, " ", gatherArguments
    if not dryRun:
        command = preexec + '; ' + gatherExe
        executeGatherScript(command, gatherArguments, len(scatterArgumentsList), base_wd)

        #process = subprocess.Popen(bsubCmd, stdout=jobSubmissionInfoFile, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)
        #process.communicate(generateGatherScript(gatherExe, gatherArguments, len(scatterArgumentsList)))
        #if process.returncode != 0:
        #    raise Exception("Non-zero return code dispatching to LSF")
    
    jobNameFile = open("jobName.txt", "w");
    print >> jobNameFile, "gather_%s" % (jobSuffix),
    jobNameFile.close()
finally:
    jobSubmissionInfoFile.close()
