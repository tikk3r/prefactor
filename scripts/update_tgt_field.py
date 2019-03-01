#! /usr/bin/env python
import argparse

import casacore.tables as ct

def main(msname, newtarget):
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
