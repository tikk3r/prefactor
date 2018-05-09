print('Submitting pref_cal1...')
print('1) Set up the sandbox')
from GRID_LRT import sandbox
sbx=sandbox.Sandbox()
sbx.build_sandbox('/home/fsweijen/software/GRID_LRT/GRID_LRT/data/config/steps/pref_cal2.cfg')
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
# Pass the SRMs from pref_cal1 here. Use the srm_from_gsiftp script.
with open('srm_pref_cal1_L261539.txt', 'r') as f:
    for l in f.readlines():
            s.append(l.strip())

print('5) Slice the list into a dictionary grouped with a group size.')
# In this case 999, so we get 1 entry.
g = s.sbn_dict(pref='SB', suff='_')
d = srmlist.slice_dicts(g, 999)

print('6) Create a token for each subband.')
ts = Token.TokenSet(th=th, tok_config='/home/fsweijen/software/GRID_LRT/GRID_LRT/data/config/steps/pref_cal2.cfg')
ts.create_dict_tokens(iterable=d, id_prefix='SB', id_append=s.OBSID, key_name='STARTSB', file_upload='srm.txt')
ts.add_keys_to_list('OBSID', s.OBSID, tok_list=None)

print('7) Attach the parset to use to tokens.')
ts.add_attach_to_list('/home/fsweijen/software/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-2.parset', tok_list=None, name='Pre-Facet-Calibrator-2.parset')
th.add_view('pref_cal2', cond='doc.PIPELINE_STEP=="pref_cal2"')
print('8) Create and launch the jobs.')
from GRID_LRT.Application import submit
j = submit.jdl_launcher(numjobs=len(d.keys()), token_type=th.t_type, wholenodes=False, parameter_step=4, NCPU=6)
with j:
    print(j.launch())
