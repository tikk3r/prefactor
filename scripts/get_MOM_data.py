#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, time
import siplib
import query
import re
import cPickle

import ssl

sip_cache = {}

#sip_cache_file = '/media/scratch/test/horneff/Pipeline-Test/feedback_test/sip_cache.pkl'
sip_cache_file = None

def init_cache(cache_file, create_if_needed=False):
    global sip_cache_file
    global sip_cache
    if os.path.exists(cache_file):
        print "Reading in SIP-cache..."
        with open(cache_file) as f:
            sip_cache = cPickle.load(f)
        print "Found %d SIPs in cache-file."%(len(sip_cache))
        sip_cache_file = cache_file
    elif (create_if_needed
          and os.path.isdir(os.path.dirname(cache_file))
          and os.access(os.path.dirname(cache_file),os.W_OK) ):
        sip_cache_file = cache_file
    else:
        raise ValueError("init_cache: cache-file does not exist!")

def get_dataID_from_filename(path, project, verbose=False):
    from common.database.Context import context
    from awlofar.config.startup import CorrelatedDataProduct,UnspecifiedDataProduct
    filename = os.path.basename(path)
    context.set_project(project)
    query = CorrelatedDataProduct.dataProductIdentifierName == filename
    for dprod in query:
        if verbose:
            print "Found dataID %s for file %s in Correlated-DataProducts"%(dprod.dataProductIdentifier,filename)
        return dprod.dataProductIdentifier
    query2 = UnspecifiedDataProduct.dataProductIdentifierName == filename
    for dprod in query2:
        if verbose:
            print "Found dataID %s for file %s in Unspecified-DataProducts"%(dprod.dataProductIdentifier,filename)
        return dprod.dataProductIdentifier    
    
def get_obsID_from_filename(path):
    filename = os.path.basename(path)
    obsReg = re.compile(r'L\d+')
    obsmatch = obsReg.match(filename)
    if (not obsmatch):
        print "get_obsID_from_filename: Could not find obs-ID in filename %s!"%(filename)
        raise ValueError("Could not find obs-ID in filename!")
    obsID = obsmatch.group(0)[1:]
    return obsID

def get_SIP_from_dataID(dpid, projectID, verbose=False):
    global sip_cache
    try:
        xml = query.getsip_fromlta_byprojectandltadataproductid(projectID, dpid)
    except:
        ssl._create_default_https_context = ssl._create_unverified_context
        xml = query.getsip_fromlta_byprojectandltadataproductid(projectID, dpid)
    new_sip = siplib.Sip.from_xml(xml)
    filename = new_sip.sip.dataProduct.fileName
    sip_cache[filename] = new_sip
    return new_sip

def get_SIPs_from_obsID(obsID, projectID, verbose=False):
    if verbose:
        print "Downloading all SIPs for \"observation\" %s in project %s."%(obsID, projectID)
    try:
        dpids = query.getltadataproductids_fromlta_byprojectandsasid(projectID, obsID)
    except:
        ssl._create_default_https_context = ssl._create_unverified_context
        dpids = query.getltadataproductids_fromlta_byprojectandsasid(projectID, obsID)
    if not dpids or len(dpids) < 1:
        print "get_SIPs_from_obsID: failed to get dataproduct-IDs for obs %s in project %s!"%(obsID, projectID)
    starttime = time.time()
    for (num,input_dpid) in enumerate(dpids):
        get_SIP_from_dataID(input_dpid, projectID, verbose)
        if verbose and ((num+1)%10)==0:
            duration = time.time()-starttime
            ETA = duration/(num+1)*(len(dpids)-num-1)
            print "read %d of %d SIPs, elapsed: %.2f seconds, ETA: %.2f seconds"%((num+1),len(dpids),duration,ETA)

def get_projectID_from_MSfile(path):
    import pyrap.tables as pt
    t = pt.table(path+"::OBSERVATION", readonly=True, ack=False)
    project = t.getcell('PROJECT',0)
    return project
            
def get_SIP_from_MSfile(path, dpID_mode="obsID", download_if_needed=True, projectID=None, verbose=False):
    if path[-1] == '/':
        path = path[:-1]
    filename = os.path.basename(path)
    nameparts = filename.split('.MS')
    if verbose and len(nameparts) < 2:
        print "get_SIP_from_MSfile: Filename \"%s\" does not look like a standard LOFAR MS file name. This may cause problems later on."%(filename)
    fileID = nameparts[0]
    strlen = len(fileID)
    for keyname in sip_cache.keys():
        if keyname[:strlen] == fileID:
            if verbose:
                print "Found SIP for %s in the cache"%(fileID)
            return sip_cache[keyname]
    if verbose:
        print "Cannot find SIP for %s in cache."%(filename)
    if not download_if_needed:
        raise ValueError("Cannot find SIP in cache and download is forbidden")
    if not projectID:
        projectID = get_projectID_from_MSfile(path)
    if dpID_mode.upper() == "OBSID":
        obsID = get_obsID_from_filename(filename)
        get_SIPs_from_obsID(obsID, projectID, verbose)
    elif dpID_mode.upper() == "LTA_NAME":
        dpID = get_dataID_from_filename(filename, projectID, verbose)
        get_SIP_from_dataID(dpID , projectID, verbose)
    else:
        raise ValueError("Unkonwn value for dpID_mode!: \""+str(dpID_mode)+"\"")
    if  sip_cache_file:
        with open(sip_cache_file,'w') as f:
            cPickle.dump(sip_cache,f,2)
    for keyname in sip_cache.keys():
        if keyname[:strlen] == fileID:
            if verbose:
                print "Found SIP for %s in the updated cache"%(fileID)
            return sip_cache[keyname]
    print "Cannot find SIP for %s in cache after downloading SIPs for obs %s!"%(filename, obsID)
    raise ValueError("Failed to download or identify SIP!")

def input2bool(invar):
    if invar == None:
        return None
    if isinstance(invar, bool):
        return invar
    elif isinstance(invar, str):
        if invar.upper() == 'TRUE' or invar == '1':
            return True
        elif invar.upper() == 'FALSE' or invar == '0':
            return False
        else:
            raise ValueError('input2bool: Cannot convert string "'+invar+'" to boolean!')
    elif isinstance(invar, int) or isinstance(invar, float):
        return bool(invar)
    else:
        raise TypeError('input2bool: Unsupported data type:'+str(type(invar)))
    
def main(MSfile, dpID_mode="obsID", outdir=".", projectID=None, download_if_needed=True,
         cache_file=None, verbose=False):
    """
    Download a SIP from MOM (or read it from the cache), write it into an 
    xml file and return the name of the created file.

    Parameters
    ----------
    MSfile : str
        Filename or srm-URL of MS for which to download the SIP
    dpID_mode : str
        How to figure out the data-product-ID:
        - "obsID" : Go via the "observation"-number in the name of the file
        - "LTA_name" : Search for the file-name in the dataProductIdentifierName on the LTA
        - "SRM" : Not yet implemented
    outdir : str
        Path to the directory where to store the xml file.
    projectID : str
        ID of the project to which the data belongs. Can be determined from the MS itself if
        it is available. (But it may be faster to specify it here anyway.)
    download_if_needed : bool
        Set to False to disable downloads.
    cache_file : str
        Path to a cache-file with downloaded SIPs. Only way to get SIPs if download is 
        disabled. 
        *Warning!* Don't use if this script is run multiple times in parallel!
    verbose : bool
        Be more verbose.

    Returns
    -------
    result : directory
        python-directory with "MOMsip" : xmlPath
    """
    download_if_needed = input2bool(download_if_needed)
    verbose = input2bool(verbose)
    MSfile = MSfile.strip()

    
    if MSfile[:6] == "srm://":
        isSRM = True
    else:
        isSRM = False

    if cache_file:
        init_cache(cache_file, create_if_needed=True)
    newsip = get_SIP_from_MSfile(MSfile, dpID_mode=dpID_mode, download_if_needed=download_if_needed,
                                 projectID=projectID, verbose=verbose)
    
    # os.path.basename() also works on SRM-URLs (at least on Linux systems)
    xmlName = os.path.basename(MSfile) + ".xml"
    xmlPath = os.path.join(outdir,xmlName)
    newsip.save_to_file(xmlPath)

    result = { 'MOMsip' : xmlPath}
    return result
    
