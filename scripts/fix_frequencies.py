from __future__ import print_function
import argparse
import ast

import casacore.tables as ct
import numpy as np

class MS:
    """ A measurement set."""
    def __init__(self, mspath):
        self.mspath = mspath
        self.sb = mspath.split('SB')[1][:3]
        self.mset = ct.table(mspath + '::SPECTRAL_WINDOW', readonly=False)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.mset.close()

    @property
    def bandwidth(self):
        #bwt = ct.table(self.mspath + '::SPECTRAL_WINDOW', ack=False)
        bw = self.mset.getcol('TOTAL_BANDWIDTH')[0]
        return bw

    @property
    def channels(self):
        #cwt = ct.table(self.mspath + '::SPECTRAL_WINDOW', ack=False)
        cw = np.unique(self.mset.getcol('CHAN_FREQ').squeeze())
        print(cw)
        return len(cw)

    def get_channels(self):
        #cwt = ct.table(self.mspath + '::SPECTRAL_WINDOW', ack=False)
        cw = self.mset.getcol('CHAN_FREQ').squeeze()
        return cw

    @property
    def channel_width(self):
        #cwt = ct.table(self.mspath + '::SPECTRAL_WINDOW', ack=False)
        cw = np.unique(self.mset.getcol('CHAN_WIDTH').squeeze())
        if len(cw) == 1:
            return cw[0]
        else:
            raise Exception('Unevenly spaced channels present!')

    def correct(self, correction_channel_width=0, correction_total_bandwidth=0, correction_ref_frequency=0, verbose=True):
        spt = self.mset
        if correction_channel_width != 0:
            channels_current = spt.getcol('CHAN_FREQ')
            widths_current = spt.getcol('CHAN_WIDTH')
            N = len(channels_current.squeeze())//2
            print('==Correcting {0:d} channel widths with {1:f} Hz.'.format(len(channels_current.squeeze()), correction_channel_width))
            corrections = np.arange(-N, N+1) * correction_channel_width
            channels_new = channels_current + corrections
            widths_new = widths_current + correction_channel_width

            if verbose:
                print('Current channels:')
                print(channels_current)
                print('Current spacing:')
                print(widths_current)
                print('Corrections:')
                print(corrections)
                print('New channels:')
                print(channels_new)
                print('New spacing:')
                print(widths_new)
                print('=======================')
                print((channels_new[0] - np.roll(channels_new[0], 1))[1:])
            spt.putcol('CHAN_FREQ', channels_new)
            spt.putcol('CHAN_WIDTH', widths_new)
        if correction_total_bandwidth != 0:
            tbw_current = self.mset.getcol('TOTAL_BANDWIDTH')
            tbw_new = tbw_current + correction_total_bandwidth
            print('==Correcting total bandwidth by {0:f} Hz.'.format(correction_total_bandwidth))
            if verbose:
                print('Current total bandwidth: {0:f} Hz.'.format(tbw_current[0]))
                print('New total bandwidth: {0:f} Hz.'.format(tbw_new[0]))
            spt.putcol('TOTAL_BANDWIDTH', tbw_new)
        if correction_ref_frequency != 0:
            rf_current = self.mset.getcol('REF_FREQ')
            rf_new = rf_current + correction_ref_frequency
            print('==Correcting reference frequency by {0:f} Hz.'.format(correction_ref_frequency))
            spt.putcol('REF_FREQ', rf_new)


    @property
    def reference_frequency(self):
        rft = ct.table(self.mspath + '::SPECTRAL_WINDOW', ack=False)
        rf = rft.getcol('REF_FREQUENCY')[0]
        return rf

def find_nearest(value, arr):
    array = np.asarray(arr)
    index = np.abs(array - value).argmin()
    return array[index]

def main(msname, correct, total_bandwidth):
    correct = ast.literal_eval(correct)
    total_bandwidth = ast.literal_eval(total_bandwidth)
    print('=')
    print('= Opening {0:s} for correction.'.format(msname))
    print('=')
    reffreqs = np.load('central_frequencies_000_319.npy')
    with MS(msname) as ms:
        print('Current total bandwidth:'.ljust(30) + '{0:.6f} Hz'.format(ms.bandwidth))
        print('Required total bandwidth:'.ljust(30) + '{0:.6f} Hz'.format(total_bandwidth))
        print('Correction:'.ljust(30) + '{0:.6f} Hz.\n'.format(total_bandwidth - ms.bandwidth))

        print('Current channel width:'.ljust(30) + '{0:.6f} Hz'.format(ms.channel_width))
        print('Required channel width:'.ljust(30) + '{0:.6f} Hz'.format(total_bandwidth / ms.channels))
        print('Correction:'.ljust(30) + '{0:.6f} Hz\n'.format(total_bandwidth / ms.channels - ms.channel_width))

        print('Current reference frequency:'.ljust(30) + '{0:.6f} Hz'.format(ms.reference_frequency))
        print('Nearest reference frequency:'.ljust(30) + '{0:.6f} Hz'.format(find_nearest(ms.reference_frequency, reffreqs)))
        print('Correction:'.ljust(30) + '{0:.6f} Hz\n'.format(find_nearest(ms.reference_frequency, reffreqs) - ms.reference_frequency))

        if correct:
            ms.correct(correction_channel_width=(total_bandwidth / ms.channels - ms.channel_width), correction_total_bandwidth=(total_bandwidth - ms.bandwidth))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Fix frequencies for the EoR data based on channel spacing and subband gap, making subbands evenly spaced.')
    parser.add_argument('measurementset', action='store', help='The measurement set to inspect or correct.')
    parser.add_argument('-c', '--correct', action='store_true', help='Apply corrections to the measurement set.',
                        default=False)
    parser.add_argument('-twb', '--total_bandwidth', action='store', help='The total sub-band bandwidth in Hz.', default=195312.5)
    args = parser.parse_args()
    #print(vars(args))
    #[print(a, v, type(v)) for a, v in vars(args).items()]
    main(args.measurementset, args.correct, args.total_bandwidth)
