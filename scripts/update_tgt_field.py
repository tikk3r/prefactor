#! /usr/bin/env python
import argparse

import casacore.tables as ct

def input2strlist_nomapfile(invar):
    """ 
    from bin/download_IONEX.py
    give the list of MSs from the list provided as a string
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

def main(mses, newtarget):
    mslist = input2strlist_nomapfile(mses)
    for msname in mslist:
        ms = msname.rstrip('/')
        tab = ct.table(ms + '::OBSERVATION', readonly=False)
        tab.putcell('LOFAR_TARGET', 0, newtarget)
        tab.close()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Fix frequencies for the EoR data based on channel spacing and subband gap, making subbands evenly spaced.')
    parser.add_argument('measurementset', action='store', help='The measurement set to inspect or correct.')
    parser.add_argument('newtarget', action='store', help='The new target field.')
    args = parser.parse_args()
    main(args.measurementset, args.newtarget)
