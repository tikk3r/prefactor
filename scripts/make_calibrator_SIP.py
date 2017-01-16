#!/usr/bin/env python

import os,sys
import siplib
import feedback
import time
import numpy as np
import get_MOM_data as MOM
import uuid
import lofarpipe.support.parset as parset
from lofarpipe.support.utilities import disk_usage

def parset_to_int(parset,key):
    if parset[key].getString().upper() == 'NONE':
        return None
    else:
        return parset[key].getInt()

def parset_to_bool(parset,key):
    if parset[key].getString().upper() == 'NONE':
        return None
    else:
        return parset[key].getBool()

def make_CalPipeline_from_parset(pipeline_name, pipeline_identifier, ID_source,
                                  starttime, duration,
                                  description_parset, input_dpids):
    """
    Create a calibration pipeline object for the calibrator pipeline

    Parameters:
    -----------
    pipeline_name : str
        Name that identifies this pipeline run.
    pipeline_ID : str
        ID for this pipeline run (not yet a siplib.Identifier object!)
    ID_source : str
        identifier source for all new identifiers in this pipeline
    starttime : str
        "start"time of this pipeline run in XML format
    duration : str
        "duration" of this pipeline run in XML format
    description_parset : parset object
        parset with additional data fro this pipeline
    input_dpids : list of siplib.Identifier objects
        Identifiers of the input data to this pipeline
    """
    new_pipeline = siplib.CalibrationPipeline(
        siplib.PipelineMap(
            name=pipeline_name,
            version=description_parset['prefactor_version'].getString(),
            sourcedata_identifiers=input_dpids,
            process_map=siplib.ProcessMap(
                strategyname=description_parset['strategyname_cal'].getString(),
                strategydescription=description_parset['strategydescription_cal'].getString(),
                starttime=starttime,
                duration=duration,
                identifier=pipeline_identifier,
                observation_identifier=pipeline_identifier,
                relations=[ ]
                #parset_source=None,
                #parset_id=None
            )
        ),
        skymodeldatabase=description_parset['skymodeldatabase_calibrator'].getString(),
        numberofinstrumentmodels=parset_to_int(description_parset,'numinstrumentmodels'),
        numberofcorrelateddataproducts=parset_to_int(description_parset,'numcorrelateddataproducts'),
        frequencyintegrationstep=parset_to_int(description_parset,'frequencyintegrationstep'),
        timeintegrationstep=parset_to_int(description_parset,'timeintegrationstep'),
        flagautocorrelations=parset_to_bool(description_parset,'flagautocorrelations'),
        demixing=parset_to_bool(description_parset,'demixing')
    )
    return new_pipeline

def make_InstrumentModelDP(DP_path, DP_identifier, Pipeline_identifier):
    """
    Create an Instrument Model Data Product object

    Parameters:
    -----------
    DP_path : str
        path to the Instrument Model
    DP_identifier : siplib.Identifier
        new identifier object for the Instrument Model
    Pipeline_identifier : siplib.Identifier
        identifier object of the pipeline that created the Instrument Model
    """
    new_DP = siplib.InstrumentModelDataProduct(
        siplib.DataProductMap(
            type="Instrument Model",
            identifier=DP_identifier,
            size=disk_usage(DP_path),
            filename=DP_path,
            fileformat="TAR",
            process_identifier=Pipeline_identifier
        )
    )
    return new_DP

def time_in_isoformat(timestamp=None):
    import datetime, time
    try:
        return datetime.datetime.utcfromtimestamp(timestamp).isoformat()
    except:
        return datetime.datetime.utcfromtimestamp(time.time()).isoformat()

def input2strlist_nomapfile(invar):
   """ 
   from bin/download_IONEX.py
   give the list of pathes from the list provided as a string
   """
   str_list = None
   if type(invar) is str:
       if invar.startswith('[') and invar.endswith(']'):
           str_list = [f.strip(' \'\"') for f in invar.strip('[]').split(',')]
       else:
           str_list = [invar.strip(' \'\"')]
   elif type(invar) is list:
       str_list = [str(f).strip(' \'\"') for f in invar]
   else:
       raise TypeError('input2strlist: Type '+str(type(invar))+' unknown!')
   return str_list



def main(cal_results_path="", input_SIP_list=[], pipeline_name="", parset_path=""):
    """
    Create the SIP for the resulting instrument data of a prefactor calibrator pipeline

    Parameters:
    -----------
    cal_results_path : str
        path to the directory with the instrument data 
        (i.e. the data that we want to ingest)
    input_SIP_list : list of str
        list of pathes of xml files that contain the SIPs of the 
        input to the pipeline run
    pipeline_name : str
        Name that identifies this pipeline run
    parset_path : str
        path to the parset with additional information about this version of prefactor
    """
    input_SIP_files = input2strlist_nomapfile(input_SIP_list)
    input_SIPs = []
    for xmlpath in input_SIP_files:
        with open(xmlpath, 'r') as f:
            input_SIPs.append(siplib.Sip.from_xml(f.read()))
        f.close()
    if not os.path.exists(cal_results_path):
        raise ValueError('make_calibrator_SIP: invalid cal_results_path')
    if len(input_SIPs) <= 0:
        raise ValueError('make_calibrator_SIP: no valid input SIPs given!')
    if len(pipeline_name) <=0:
        raise ValueError('make_calibrator_SIP: invalid pipeline_name')
    if not os.path.exists(parset_path):
        raise ValueError('make_calibrator_SIP: invalid parset_path')    
    pipeline_parset = parset.Parset(parset_path)
    identifier_source = pipeline_parset['identifier_source'].getString()
    product_ID = "data"+str(uuid.uuid4())
    pipeline_ID = "pipe"+str(uuid.uuid4())
    product_identifier = siplib.Identifier(id=product_ID, source=identifier_source)
    pipeline_identifier = siplib.Identifier(id=pipeline_ID, source=identifier_source)
    new_product = make_InstrumentModelDP(cal_results_path, product_identifier, pipeline_identifier)
    newsip = siplib.Sip(
        project_code=input_SIPs[0].sip.project.projectCode,
        project_primaryinvestigator=input_SIPs[0].sip.project.primaryInvestigator,
        project_contactauthor=input_SIPs[0].sip.project.contactAuthor,
        #project_telescope="LOFAR",
        project_description=input_SIPs[0].sip.project.projectDescription,
        project_coinvestigators=input_SIPs[0].sip.project.coInvestigator,
        dataproduct = new_product
    )
    input_DPs = []
    for inputSIP in input_SIPs:
        newsip.add_related_dataproduct_with_history(inputSIP)
        input_DPs.append( siplib.Identifier(id=inputSIP.sip.dataProduct.dataProductIdentifier.identifier ,
                                            source=inputSIP.sip.dataProduct.dataProductIdentifier.source ,
                                            name=inputSIP.sip.dataProduct.dataProductIdentifier.name ))
    starttime = time_in_isoformat()
    duration = 'P0Y0M0DT1H'
    pipeline_parset.replace('numinstrumentmodels','1')
    pipeline_parset.replace('numcorrelateddataproducts','0')
    new_pipeline = make_CalPipeline_from_parset(pipeline_name, pipeline_identifier, identifier_source,
                                                starttime, duration,
                                                pipeline_parset, input_DPs)
    newsip.add_pipelinerun(new_pipeline)
    
    new_xml_name = cal_results_path.rstrip('/')+".xml"
    newsip.save_to_file(new_xml_name)

    result = { "new_xml_name" : new_xml_name }
    return result
    

    
