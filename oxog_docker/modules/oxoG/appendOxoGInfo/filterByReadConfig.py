'''
Created on May 19, 2012

@author: lichtens
'''
import csv
import argparse
import sys 

import subprocess 
from xml.dom.expatbuilder import Rejecter

if not (sys.version_info[0] == 2  and sys.version_info[1] in [7]):
    raise "Must use Python 2.7.x"

def call(command, isPrintCmd=True):
    ''' returns returncode, output 
    '''
    try:
        if isPrintCmd:
            print command
        return 0, subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as cpe:
        return cpe.returncode,cpe.output
    
def createPruningDict(filename):
    ''' Given a filename of oxoGMetric data, create a dictionary of chr:position to prune.'''
    result = dict()
    
    oxoGFP = file(oxoGInputFile, 'r')
    oxoTSVReader = csv.DictReader(oxoGFP, delimiter='\t')

    print("Chromosome" + "\t" + "Start_position" + "\t" + "filter" + "\t" + "value" + "\t" +  "tumor_lod" + "\t" + "i_t_NaltArt" + "\t" + "i_t_NaltTot" + "\n")

    for line in oxoTSVReader:
        ref = line['ref']

        A_F1R2 = int(line['F1_A']) + int(line['R2_A'])
        A_F2R1 = int(line['F2_A']) + int(line['R1_A'])
        C_F1R2 = int(line['F1_C']) + int(line['R2_C'])
        C_F2R1 = int(line['F2_C']) + int(line['R1_C'])
        G_F1R2 = int(line['F1_G']) + int(line['R2_G'])
        G_F2R1 = int(line['F2_G']) + int(line['R1_G'])
        T_F1R2 = int(line['F1_T']) + int(line['R2_T'])
        T_F2R1 = int(line['F2_T']) + int(line['R1_T'])

        contig = line['contig']
        contig = contig.replace('chr','').strip(' \t\n\r')
        
        # band-aid dictionary hash key to match convention in main
        if 'M' == contig:
        	contig='MT'
        
        result[contig + ":" + line['position']] = A_F1R2,A_F2R1,C_F1R2,C_F2R1,G_F1R2,G_F2R1,T_F1R2,T_F2R1
        #print contig + ":" + line['position'] 
    return result

def parseOptions():
    description = '''Given a tsv call_stats file with headers on the first line, run each mutation through the tumor_lod based oxoG Filter (second incarnation).
    This script is still useful for the third incarnation of the oxoG filter, since it can generate an unfiltered input maf file for that filter.  See --onlyAddColumnsToCopy.
    
    '''
    
    epilog= '''
        Please note that this script assumes that all comment lines are at the top of a file before the header.  Comment lines start with '#'
        
        All output files have the following columns appended:
        'i_t_ALT_F1R2' -- number of alternate reads in f1r2 configuration
        'i_t_ALT_F2R1' -- number of alternate reads in f2r1 configuration
        'i_t_REF_F1R2' -- number of alternate reads in f1r2 configuration
        'i_t_REF_F2R1' -- number of alternate reads in f2r1 configuration
        'i_t_Foxog' -- Ratio of Oxo oriented alt reads/total number of alt reads.
            "Oxo oriented alt reads":  If mutations is C>A:  F2R1.  If mutation is G>T: F1R2
            For non-oxoG mutations (i.e. ones that are neither C>A nor G>T), FoxoG is calculated with the numerator:  
            C>anything --> F2R1
            A>anything --> F2R1
            G>anything --> F1R2
            T>anything --> F1R2
        
        Writes two additional files that contain the pass and reject counts (and nothing more in each).  Those file names are:
        [outputFile].pass_count.txt
        [outputFile].reject_count.txt
        
        Required columns in the input file (case sensitive):
            'Reference_Allele'
            'Tumor_Seq_Allele2'
            'Chromosome'
            'Start_position'
            'i_t_lod_fstar' -- required when onlyAddColumnsToCopy is NOT specified and criteria is 'xART.tLOD'
            '''
    parser = argparse.ArgumentParser(description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('oxoGInputFile', metavar='oxoGInputFile',  type=str, help ='Output file from OxoGMetric')
    parser.add_argument('inputFile', metavar='inputFile', type=str, help ='input file that MUST be an annotated maf.')
    parser.add_argument('outputFile', metavar='outputFile', type=str, help='output annotated maf')
    parser.add_argument('-criteria', metavar='criteria', type=str, help='artifact cut critera',default='xART.tLOD')
    parser.add_argument('-p1', metavar='parameter1', type=float, help='criteria parameter 1',default=-10.0)
    parser.add_argument('-p2', metavar='parameter2', type=float, help='criteria parameter 2',default=33.3)
    parser.add_argument('-p3', metavar='parameter3', type=float, help='criteria parameter 3',default=0.0)
    parser.add_argument('--onlyAddColumnsToCopy', action='store_true', help='If true, creates a copy of the inputFile that contains the additional columns in the outputFile.  Also, does not generate any filtered results.  I.e. only generates a copy of the inputFile with the additional columns and performs no filtering.  No additional files (counts nor reject maf) are written', default=False)
    args = parser.parse_args()
    
    return args 

if __name__ == '__main__':
    
    args = parseOptions()
    inputFile = args.inputFile
    oxoGInputFile = args.oxoGInputFile
    outputFile = args.outputFile
    criteria = args.criteria
    p1 = args.p1
    p2 = args.p2
    p3 = args.p3

    # These are the new columns added by this script.  Please note that order is very important. 
    additionalColumns = ['i_t_ALT_F1R2','i_t_ALT_F2R1','i_t_REF_F1R2','i_t_REF_F2R1','i_t_Foxog']

    isCopyCreation = args.onlyAddColumnsToCopy
    if isCopyCreation:
        print("Generating a copy of the input file with additional columns.  This will be written to the specified output file: " + outputFile)
        
    isSkipFiltering = args.onlyAddColumnsToCopy
    if isSkipFiltering:
        print("No filtering being performed.  tumor_lod is not being used and will show up as -1 in stdout.  tumor_lod will be preserved in output, though.")

    # load the input files
    inputFileFP = file(inputFile, 'r')
    
    # Create the output file
    outputFileFP = file(outputFile, 'w')    
    
    # Create the reject maf output file
    rejectFilename = outputFile + '.reject.maf.annotated'
    if isSkipFiltering:
        rejectFilename = '/dev/null'
    
    outputFileRejectFP = file(rejectFilename, 'w')
    
    # TODO: Need a Python class that can handle comment lines (google search found plenty), instead of this two pass approach.
    # Read preceding comment lines until all comment lines have been read.  Write the comment lines to the output file.
    numComments = 0
    line = inputFileFP.readline()
    while line.startswith('#'):
        numComments = numComments + 1
        
        # Write comments to both the output and the reject files
        outputFileFP.write(line)
        outputFileRejectFP.write(line)
        
        line = inputFileFP.readline()
    inputFileFP.seek(0,0)
    
    # Read until all comment lines have been read.  Discard comment lines
    for i in range(0,numComments):
        inputFileFP.readline()
    
    inputTSVReader = csv.DictReader(inputFileFP, delimiter='\t')

    # The output fields are going to be the input 
    outfields=inputTSVReader.fieldnames
    outfields.extend(additionalColumns)


    tsvWriter = csv.DictWriter(outputFileFP, outfields, delimiter='\t', lineterminator='\n')
    tsvWriter.writeheader()
    
    # Create reject output file
    tsvRejectWriter = csv.DictWriter(outputFileRejectFP, inputTSVReader.fieldnames, delimiter='\t', lineterminator='\n')
    tsvRejectWriter.writeheader()
    
    pruningDict = createPruningDict(oxoGInputFile)
    passCount = 0
    rejectCount = 0
    # When a mutation is pruned out, rewrite the line. 
    for line in inputTSVReader:
        ref_allele = line['Reference_Allele']
        alt_allele = line['Tumor_Seq_Allele2']
        

        filter = False;
        value = -1
        i_t_ALT_F1R2=0
        i_t_ALT_F2R1=0
        i_t_REF_F1R2=0
        i_t_REF_F2R1=0

        # band-aid broken MT reference dictionary
        if 'M' is line['Chromosome']:
                line['Chromosome']='MT'

        key = line['Chromosome'] + ":" + line['Start_position']
        try:
                A_F1R2,A_F2R1,C_F1R2,C_F2R1,G_F1R2,G_F2R1,T_F1R2,T_F2R1 = pruningDict[key]
        except:
                print("pos "+ key +" not in dictionary - skipping!!")
                continue

        if (alt_allele == 'A'):
            i_t_ALT_F1R2=A_F1R2
            i_t_ALT_F2R1=A_F2R1
        if (alt_allele == 'C'):
            i_t_ALT_F1R2=C_F1R2
            i_t_ALT_F2R1=C_F2R1
        if (alt_allele == 'G'):
            i_t_ALT_F1R2=G_F1R2
            i_t_ALT_F2R1=G_F2R1
        if (alt_allele == 'T'):
            i_t_ALT_F1R2=T_F1R2
            i_t_ALT_F2R1=T_F2R1
        if (ref_allele == 'A'):
            i_t_REF_F1R2=A_F1R2
            i_t_REF_F2R1=A_F2R1
        if (ref_allele == 'C'):
            i_t_REF_F1R2=C_F1R2
            i_t_REF_F2R1=C_F2R1
        if (ref_allele == 'G'):
            i_t_REF_F1R2=G_F1R2
            i_t_REF_F2R1=G_F2R1
        if (ref_allele == 'T'):
            i_t_REF_F1R2=T_F1R2
            i_t_REF_F2R1=T_F2R1

        line1=line;
        line1['i_t_ALT_F1R2'] = str(i_t_ALT_F1R2)
        line1['i_t_ALT_F2R1'] = str(i_t_ALT_F2R1)
        line1['i_t_REF_F1R2'] = str(i_t_REF_F1R2)
        line1['i_t_REF_F2R1'] = str(i_t_REF_F2R1)

        Noxog=0
        Nalt=i_t_ALT_F2R1+i_t_ALT_F1R2;

        if ((ref_allele == 'C') or (ref_allele=='A')):
            Noxog=i_t_ALT_F2R1

        if ((ref_allele == 'G') or (ref_allele=='T')):
            Noxog=i_t_ALT_F1R2
        
        isArtifactSignature = ((ref_allele == 'C') and (alt_allele=='A')) or ((ref_allele == 'G') and (alt_allele=='T'))
        
        tumor_lod = -1
        
        if (Nalt>0):
            value = float(Noxog)/float(Nalt)
      
            if isArtifactSignature and not isSkipFiltering:
                if (criteria=='xART.tLOD'):
                    tumor_lod = float(line['i_t_lod_fstar'])
                    filter = tumor_lod < (p1+p2*value)
                

        line1['i_t_Foxog'] = str(value)

        #outputFileFP.write(line['Chromosome'] + "\t" + line['Start_position'] + "\t" + str(filter) + "\n")
        print((line1['Chromosome'] + "\t" + line1['Start_position'] +"\t" + ref_allele+alt_allele+ "\t" + str(filter) + "\t" + str(value) + "\t" +  str(tumor_lod) + "\t" + line1['i_t_ALT_F1R2'] + "\t" + line1['i_t_ALT_F2R1']))

        if not filter:
            tsvWriter.writerow(line1)
            passCount = passCount + 1 
        else:
            tsvRejectWriter.writerow(line1)
            rejectCount = rejectCount + 1 
        
    outputFileFP.close()
    
    # Write the pass and reject counts to separate files
    if not isSkipFiltering:
        fpPassCount = file(outputFile + ".pass_count.txt", 'w')
        fpPassCount.write(str(passCount))
        fpPassCount.close()
        
        fpRejectCount = file(outputFile + ".reject_count.txt", 'w')
        fpRejectCount.write(str(rejectCount))
        fpRejectCount.close()
    
    pass
