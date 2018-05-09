print('Submitting pref_cal1...')
print('1) Set up the sandbox')
from GRID_LRT import sandbox
sbx=sandbox.Sandbox()
sbx.build_sandbox('/home/fsweijen/software/GRID_LRT/GRID_LRT/data/config/steps/pref_cal1.cfg')
sbx.upload_sandbox()
sbx.cleanup()

print('2) Get picas credentials to log in with.')
from GRID_LRT.get_picas_credentials import picas_cred
pc = picas_cred()

print('3) Set up a Token Handler.')
from GRID_LRT import Token
th=Token.Token_Handler( t_type="test_EoR", srv="https://picas-lofar.grid.surfsara.nl:6984", uname=pc.user, pwd=pc.password, dbn=pc.database)
th.add_overview_view()
th.add_status_views()

print('4) Create a list of paths to the files.')
from GRID_LRT.Staging import srmlist
s = srmlist.srmlist()
with open('calib_srm.txt', 'r') as f:
    for l in f.readlines():
            s.append(l.strip())

print('5) Slice the list into a dictionary grouped with a group size.')
# In this case 1, so we get N_SB entries.
g = s.sbn_dict(pref='SB', suff='_')
d = srmlist.slice_dicts(g, 1)

print('6) Create a token for each subband.')
ts = Token.TokenSet(th=th, tok_config='/home/fsweijen/software/GRID_LRT/GRID_LRT/data/config/steps/pref_cal1.cfg')
ts.create_dict_tokens(iterable=d, id_prefix='SB', id_append=s.OBSID, key_name='STARTSB', file_upload='srm.txt')
ts.add_keys_to_list('OBSID', s.OBSID, tok_list=None)

print('7) Attach the parset to use to tokens.')
ts.add_attach_to_list('/home/fsweijen/software/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-1.parset', tok_list=None, name='Pre-Facet-Calibrator-1th.add_view('pref_targ1', cond='doc.PIPELINE_STEP=="pref_targ1"').parset')
th.add_view('pref_cal1', cond='doc.PIPELINE_STEP=="pref_cal1"')

print('8) Create and launch the jobs.')
from GRID_LRT.Application import submit
j = submit.jdl_launcher(numjobs=len(d.keys()), token_type=th.t_type, wholenodes=False, parameter_step=4, NCPU=2)
with j:
    print(j.launch())
