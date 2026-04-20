import ssl
ssl._create_default_https_context = ssl._create_unverified_context
from astroquery.simbad import Simbad
import warnings; warnings.filterwarnings("ignore")

q = "SELECT main_id, otype FROM basic WHERE main_id IN ('* alf UMa', '* bet UMa', '* eps UMa', '* eta UMa', '* gam UMa', '* zet UMa', '* del UMa')"
t = Simbad.query_tap(q)
for row in t:
    print(row['main_id'], "->", row['otype'])
