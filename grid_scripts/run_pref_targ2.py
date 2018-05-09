print '1) Set up the sandbox.'
from GRID_LRT import sandbox
sbx=sandbox.Sandbox()
sbx.build_sandbox('/home/fsweijen/software/GRID_LRT/GRID_LRT/data/config/steps/pref_targ2.cfg')
sbx.upload_sandbox()
sbx.cleanup()

print '2) Get picas credentials to log in with.'
from GRID_LRT.get_picas_credentials import picas_cred
pc = picas_cred()

print '3) Set up a Token Handler.'
from GRID_LRT import Token
th=Token.Token_Handler( t_type="test_EoR", srv="https://picas-lofar.grid.surfsara.nl:6984", uname=pc.user, pwd=pc.password, dbn=pc.database)
th.add_overview_view()
th.add_status_views()
#th.reset_tokens('error')

print '4) Create a list of paths to the files.'
from GRID_LRT.Staging import srmlist
s = srmlist.srmlist()
with open('srm_pref_targ1_L261539-test.txt', 'r') as f:
    for l in f.readlines():
            s.append(l.strip())

print '5) Slice the list into a dictionary grouped with a group size 10.'
g = s.sbn_dict(pref='AB', suff='_')
d = srmlist.slice_dicts(g, 10)

print '6) Create a token for each subband.'
ts = Token.TokenSet(th=th, tok_config='/home/fsweijen/software/GRID_LRT/GRID_LRT/data/config/steps/pref_targ2.cfg')
ts.create_dict_tokens(iterable=d, id_prefix='AB', id_append=s.OBSID, key_name='STARTSB', file_upload='srm.txt')

ts.add_keys_to_list('OBSID', s.OBSID, tok_list=None)
ts.add_keys_to_list('CAL_OBSID', s.OBSID, tok_list=None)
th.add_view('pref_targ2', cond='doc.PIPELINE_STEP=="pref_targ2"')

print '7) Attach the parset to use to tokens.'
ts.add_attach_to_list('/home/fsweijen/software/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Target-2-EoR.parset', tok_list=None, name='Pre-Facet-Target-2-EoR.parset')


print '8) Create and launch the jobs.'
from GRID_LRT.Application import submit
j = submit.jdl_launcher(numjobs=len(d.keys()), token_type=th.t_type, wholenodes=False, parameter_step=1, NCPU=8)

with j:
    print 'pref_targ2 run:', j.launch()
