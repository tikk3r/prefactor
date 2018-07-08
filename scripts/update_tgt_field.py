#! /usr/bin/env python
import argparse

import casacore.tables as ct

def main(msname, newtarget):
    ms = msname.rstrip('/')
    ct.taql('UPDATE LOFAR_TARGET SET ' + newtarget + 'FROM ' + ms + '::OBSERVATION')
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Fix frequencies for the EoR data based on channel spacing and subband gap, making subbands evenly spaced.')
    parser.add_argument('measurementset', action='store', help='The measurement set to inspect or correct.')
    parser.add_argument('newtarget', action='store', help='The new target field.')
    args = parser.parse_args()
    main(args.measurementset, args.newtarget)
