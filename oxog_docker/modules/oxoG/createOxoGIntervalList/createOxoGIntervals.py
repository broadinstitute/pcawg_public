'''
Created on May 18, 2012

@author: lichtens
'''

import csv
import argparse
import sys 
import re
import tempfile
import subprocess 
import os

if not (sys.version_info[0] == 2  and sys.version_info[1] in [7]):
    raise "Must use Python 2.7.x"


def createOxoGIntervalsFile(inputFile, outputIntervalFileName):
    outFP = file(outputIntervalFileName, 'w')
    inputFP = file(inputFile, 'r')
    inputFP = removeCommentLines(inputFP)
    inputTSVReader = csv.DictReader(inputFP, delimiter='\t')
    mouse = False
    # Read in the call stats file and for each chr, start row, add it to the interval file (of length 1)
    #    contig  position
    for line in inputTSVReader:
        #print line.keys()
        if 'contig' in line.keys():
            chrom = line['contig']
            position = line['position']
        elif 'chr' in line.keys():
            chrom = line['chr']
            position = line['start']
        else:
            chrom = line['Chromosome']
            position = line['Start_position']
            # mm8 MAFs need a 'chr' in the interval_list
            if 'NCBI_Build' in line.keys():
                mouse = line['NCBI_Build'] == 'mm9'
                if (mouse):
                    chrom = 'chr' + chrom
        # band-aid broken MT reference dictionary
        # MAF
        if ('M' in chrom) and (not mouse):
            chrom = 'MT'
        #if ('judgement' not in line.keys()) or (line['judgement'] == 'KEEP'):
        outFP.write(chrom + ":" + position + "\n")
    
        #else:
            #outRejectFP.write(chrom + ":" + position + "\n")
    outFP.close()

def call(command, isPrintCmd=True):
    ''' returns returncode, output 
    '''
    try:
        if isPrintCmd:
            print command
        return 0, subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as cpe:
        return cpe.returncode,cpe.output

def parseOptions():
    description = '''Given a tsv call_stats file with headers on the first line, run each mutation through the .'''
    epilog= '''
        '''
    parser = argparse.ArgumentParser(description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('inputFile', metavar='inputFile',  type=str, help ='call stats file from MuTect or an annotated maf from Oncotator ')
    parser.add_argument('outputIntervalFileName', metavar='outputIntervalFileName', type=str, help ='')
    #parser.add_argument('outputIntervalRejectFileName', metavar='outputIntervalRejectFileName', type=str,default='', help ='Only used if input is a call_stats file (or other file with judgement column)')
    
    args = parser.parse_args()
    
    return args 

def removeCommentLines(inputFP, commentPrepend="#"):
    ''' Removes lines starting with the comment prepend.  
    Note that any space before the comment prepend character will throw this method off '''
    resetLocation = inputFP.tell()
    nextChar = inputFP.read(1)
    commentLines = ""
    
    # Get rid of blank lines
    while nextChar in ['\n', '\r']:
        resetLocation = inputFP.tell()
        nextChar = inputFP.read(1)
        
    while (nextChar == commentPrepend):
        commentLines = commentLines + (commentPrepend + inputFP.readline())
        resetLocation = inputFP.tell()
        nextChar = inputFP.read(1)
    
    # Go back one character to make sure that we have moved the file pointer to the beginning of the first non-comment line.
    inputFP.seek(resetLocation, os.SEEK_SET)
    return inputFP
    
if __name__ == '__main__':
    
    args = parseOptions()
    inputFile = args.inputFile
    outputIntervalFileName = args.outputIntervalFileName
    #outputIntervalRejectFileName = args.outputIntervalRejectFileName
    
    createOxoGIntervalsFile(inputFile, outputIntervalFileName)
    