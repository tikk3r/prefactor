import os
from lofarpipe.support.data_map import DataMap
from lofarpipe.support.data_map import DataProduct


def plugin_main(args, **kwargs):
    """
    create the name for the results directory of a calibrator pipeline

    Parameters
    ----------
    mapfile_in : str
        Filename of datamap with the input files
    extension : str
        extension to add to the output-names
    ingest_directory : str
        Directory in which to put the tar-ball
    mapfile_dir : str
        Directory in which to put the created mapfiles

    Returns
    -------
    result : dict
        New datamap filename

    """
    mapfile_in = kwargs['mapfile_in']
    extension = kwargs['extension']
    ingest_directory = kwargs['ingest_directory']
    mapfile_dir = kwargs['mapfile_dir']

    map_in = DataMap.load(mapfile_in)

    observation_list = []
    for i, item in enumerate(map_in):
        obs_name = item.file.split("_")[0]
        if not obs_name in observation_list:
            observation_list.append(obs_name)
    observation_list.sort()
    
    newname = ""
    for obsname in observation_list:
        newname += obsname+"_"
    newname = newname.strip('_')
    newname += extension

    tar_name = newname + '.tgz'
    tar_name = os.path.join(ingest_directory,tar_name)
    
    map_out = DataMap([])
    map_out.data.append(DataProduct("localhost", newname, False))
    fileid = os.path.join(mapfile_dir, "makeCalResultsDirname.mapfile")
    map_out.save(fileid)

    map_tar_out = DataMap([])
    map_tar_out.data.append(DataProduct("localhost", tar_name, False))
    fileid_tar = os.path.join(mapfile_dir, "makeCalResultsDirname.mapfile")
    map_tar_out.save(fileid_tar)

    result = {'dirname': fileid , 'tarname' : fileid_tar}

    return result
