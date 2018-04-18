#!/usr/bin/env python
"""
Script to correct the frequencies of MS 
observed during LOFAR cycle 0.
"""
from __future__ import print_function
import os
import numpy as np
from pyrap import tables
import argparse
from glob import glob
import re


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def main(ms):
    """
    Function to be called from the pipeline
    """
    correct_ms(ms)

def read_ms(ms):
    """
    Read the frequency center for each channel and the widths.
    Return also the frequency spacing
    """
    msfr = tables.table(os.path.join(ms, "SPECTRAL_WINDOW"), 
                        readonly=True)
    frequencies = msfr.getcol('CHAN_FREQ')[0]
    spacing = frequencies[1:]-frequencies[:-1]
    widths = msfr.getcol("CHAN_WIDTH")[0]
    ref_frequency = msfr.getcol('REF_FREQUENCY')[0]
    total_bandwidth = msfr.getcol('TOTAL_BANDWIDTH')[0]
    msfr.close()
    return frequencies, spacing, widths, ref_frequency, total_bandwidth

def write_ms(ms, freqs, widths=None, ref_frequency=None, total_bandwidth=None):
    """
    Write the frequency center for each channel and the widths.
    """
    msfr = tables.table(os.path.join(ms, "SPECTRAL_WINDOW"), 
                        readonly=False)
    # Check size of the freqs
    aux_freqs = np.expand_dims(freqs, axis=0)
    msfr.putcol('CHAN_FREQ', aux_freqs)
    if widths is not None:
        aux_widths = np.expand_dims(widths, axis=0)
        msfr.putcol('CHAN_WIDTH', aux_widths)
        msfr.putcol('EFFECTIVE_BW', aux_widths)
        msfr.putcol('RESOLUTION', aux_widths)
    if ref_frequency is not None:
        msfr.putcol('REF_FREQUENCY', ref_frequency)
    if total_bandwidth is not None:
        msfr.putcol('TOTAL_BANDWIDTH', total_bandwidth)

def get_central_freq(group, sb_per_group=10, corr_factor=0):
    """
    Get the central frequency of a group
    """
    mfreq = np.load(os.path.join(THIS_DIR, "mfreq.npy")).astype("Float64")
    if sb_per_group % 2 == 0:
        return (mfreq[group*sb_per_group+sb_per_group//2-1+corr_factor]+
                mfreq[group*sb_per_group+sb_per_group//2+corr_factor])/2.
    else:
        return mfreq[group*sb_per_group+sb_per_group//2+corr_factor]

def compute_freqs(group, sb_per_group=10, channels_per_group=50):
    """
    Compute the even spacing of frequencies for a given group
    """
    if (sb_per_group == 10) and (group >= 32): # Drop wrong band
        central_freq = get_central_freq(group, sb_per_group=sb_per_group, corr_factor=1)
    else:
        central_freq = get_central_freq(group, sb_per_group=sb_per_group)
        
    
    # Heuristics for the channel positions
    # TODO: Correct for sb_per_group different than 10 or 1
    if sb_per_group == 10:
        if group < 30:
            central_freq_aux = get_central_freq(group+1, sb_per_group=sb_per_group)
        elif group == 32:
            ## WARNING
            central_freq_aux = get_central_freq(group-1, sb_per_group=sb_per_group, corr_factor=1)
        elif group == 33:
            central_freq_aux = get_central_freq(group+1, sb_per_group=sb_per_group, corr_factor=1)
        elif (group > 33) and (group <= 37):
            central_freq_aux = get_central_freq(group-1, sb_per_group=sb_per_group, corr_factor=1)
        else:
            ## ERROR
            central_freq_aux = get_central_freq(group-1, sb_per_group=sb_per_group)
    elif sb_per_group == 1:
        if (group < 319) or (group == 321):
            central_freq_aux = get_central_freq(group+1, sb_per_group=sb_per_group)
        elif (group > 321) or (group == 319):
            central_freq_aux = get_central_freq(group-1, sb_per_group=sb_per_group)
        else: # Group 320 or out of bounds
            ## ERROR
            central_freq_aux = get_central_freq(group+1, sb_per_group=sb_per_group)
    else:
        central_freq_aux = get_central_freq(group+1, sb_per_group=sb_per_group)
    # Compute the channel step
    cstep = np.absolute(central_freq - central_freq_aux)/channels_per_group
    channel_shift = channels_per_group/2-0.5
    freq_comp = np.linspace(central_freq-channel_shift*cstep, 
                           central_freq+channel_shift*cstep, 
                           channels_per_group,
                           dtype="Float64")
    return freq_comp

def get_channels_per_group(ms):
    """
    Get the number of channels per group from a MS
    """
    frequencies, spacing, widths, ref_frequency, total_bandwidth = read_ms(ms)
    return len(frequencies)

def get_group_sb(ms):
    """
    Get the group and number of sub-bands per group from the MS name
    """
    result = re.findall("_SBgr(\d+)-(\d+)_", ms)
    result_single = re.findall("_SB(\d+)_", ms)
    if result:
        group = int(result[0][0])
        sb_per_group = int(result[0][1])
    elif result_single:
        group = int(result_single[0])
        sb_per_group = 1
    else:
        group = None
        sb_per_group = None
    return group, sb_per_group

def get_info(ms, read_cpg=True):
    """
    Get the group, number of sub-bands per group and number of channels 
    per groups for a given ms
    """
    group, sb_per_group = get_group_sb(ms)
    if group is None:
        raise ValueError("Imposible to determine group from ms name")
        
    if read_cpg:
        channels_per_group = get_channels_per_group(ms)
    else:
        channels_per_group = None
    return group, sb_per_group, channels_per_group

def prepare_freqs(ms, group=None, sb_per_group=None):
    # Detect the group name.
    # Compute the frequencies based on the group name.
    if (group is None) or (sb_per_group is None):
        group_extracted, sb_per_group_extracted, channels_per_group = get_info(ms)
        if group is None:
            group = group_extracted
        if sb_per_group is None:
            sb_per_group = sb_per_group_extracted
    else:
        channels_per_group = get_channels_per_group(ms)
    return compute_freqs(group, 
                         sb_per_group=sb_per_group, 
                         channels_per_group=channels_per_group)

def show_ms(ms, machine_readable=False, group=None, sb_per_group=None):
    frequencies, spacing, widths, ref_frequency, total_bandwidth = read_ms(ms)
    print("Ref. frequency: {}; Total bandwidth: {}".format(ref_frequency, total_bandwidth))
    print("Frequencies ({})".format(len(frequencies)))
    print(frequencies)
    print("Spacing")
    print(spacing)
    print("Widths")
    print(widths)
    print("Total widths: {}; Total_bandwidth: {}; difference: {}".format(np.sum(widths), 
                                                                         total_bandwidth, 
                                                                         np.sum(widths)-total_bandwidth))
           
    freq_comp = prepare_freqs(ms, group=group, sb_per_group=sb_per_group)

    print("Computed frequencies")
    print(freq_comp)
    print("Frequency correction")
    print(frequencies-freq_comp)
    print("Computed ref. frequency")
    print(np.mean(freq_comp))
    print("Ref. requency correction")
    print(ref_frequency-np.mean(freq_comp))


def correct_ms(ms, w=False, rf=False, tb=False, group=None, sb_per_group=None):
    """
    Correct the frequencies of a given MS.
    It could also correct the widths.
    """
    freq_comp = prepare_freqs(ms, group=group, sb_per_group=sb_per_group)
    write_ms(ms, freq_comp)
    if w or rf or tb:
        frequencies, spacing, widths, ref_frequency, total_bandwidth = read_ms(ms)
        widths_new = None
        ref_frequency_new=None
        total_bandwidth_new=None
    if w:
        widths_new = np.zeros_like(widths)
        widths_new[:-1] = spacing
        widths_new[-1] = spacing[-1]
    if rf:
        ref_frequency_new = np.mean(frequencies)
    if tb:
        total_bandwidth_new = np.sum(widths)
    if w or rf or tb:
        write_ms(ms, freq_comp, 
                 widths=widths_new, 
                 ref_frequency=ref_frequency_new,
                 total_bandwidth=total_bandwidth_new)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check the frequency properties of MS files')
    parser.add_argument('-d', '--directory', action='store_true', help='directory with MS')
    parser.add_argument('-c', '--correct', action='store_true', help='Correct the frequencies of the MS')
    parser.add_argument('-w', '--widths', action='store_true', help='Correct the widths of the MS')
    parser.add_argument('-r', '--ref-frequency', action='store_true', help='Correct the REF_FREQUENCY of the MS')
    parser.add_argument('-t', '--total-bandwidth', action='store_true', help='Correct the TOTAL_BANDWIDTH of the MS')
    parser.add_argument('--group', type=int, help='Override group of the MS')
    parser.add_argument('--sb-per-group', type=int, help='Override sb per group of the MS')
    parser.add_argument('ms', help='MS or directory')
    args = parser.parse_args()
    # TODO: Check directory
    if args.directory:
        list_ms = glob(args.ms+"/*.ms")
        list_ms2 = glob(args.ms+"/*.MS")
        list_ms.extend(list_ms2)
        for ms in list_ms:
            if args.correct:
                correct_ms(ms, 
                           w=args.widths, 
                           rf=args.ref_frequency, 
                           tb=args.total_bandwidth)
            else:
                show_ms(ms)
    else:
        if args.correct:
            correct_ms(args.ms, 
                       w=args.widths, 
                       rf=args.ref_frequency, 
                       tb=args.total_bandwidth,
                       group=args.group,
                       sb_per_group=args.sb_per_group)
        else:
            show_ms(args.ms, 
                    group=args.group,
                    sb_per_group=args.sb_per_group)