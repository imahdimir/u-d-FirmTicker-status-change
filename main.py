"""

    """

import asyncio
from functools import partial

import pandas as pd
from githubdata import GithubData
from mirutil import async_requests as areq
from mirutil.df_utils import read_data_according_to_type as read_data
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil import utils as mu
from mirutil.jdate import make_zero_padded_jdate_ie_iso_fmt
from aiohttp import ClientSession
import nest_asyncio


nest_asyncio.apply()

targ_rp_url = 'https://github.com/imahdimir/d-firm-status-change'
t2f_url = 'https://github.com/imahdimir/d-TSETMC_ID-2-FirmTicker'

btic = 'BaseTicker'
tic = 'Ticker'
tid = 'TSETMC_ID'
url = 'url'
nsta = 'NewStatus'
time = 'Time'
jdt = 'JDate'
row = 'Row'
jdtime = 'JDateTime'

status_chng_burl = 'http://tsetmc.com/Loader.aspx?Partree=15131L&i='

def make_status_change_url(tsetmc_id) :
  return f'{status_chng_burl}{tsetmc_id}'

def build_df_for_each_id(id , resp_text) :
  dfs = pd.read_html(resp_text)

  assert len(dfs) == 1

  df = dfs[0]
  df[tid] = id
  df[row] = df.index

  return df

async def get_resps_for_none_vals(df , col = 'r') :
  msk = df[col].isna()
  _df = df[msk]

  fu = partial(areq.get_reps_texts_async , trust_env = True)

  cls = mu.return_clusters_indices(_df)

  for se in cls :
    si = se[0]
    ei = se[1]
    inds = _df.iloc[si : ei + 1].index
    print(inds)

    urls = df.loc[inds , url]

    out = await fu(urls)

    df.loc[inds , 'r'] = out

    # break

  return df

def main() :
  pass
  ##
  rp_t2f = GithubData(t2f_url)
  rp_t2f.clone()
  ##
  dftfp = rp_t2f.data_fp
  dft = read_data(dftfp)
  ##
  dft = dft[[tid]]
  dft.drop_duplicates(inplace = True)
  dft.dropna(inplace = True)
  ##
  dft[url] = dft[tid].apply(make_status_change_url)
  ##
  dft['r'] = None
  ##
  for _ in range(3) :
    dft = asyncio.run(get_resps_for_none_vals(dft))

  ##
  msk = dft['r'].isna()
  df1 = dft[msk]
  len(msk[msk])
  ##
  sdf = pd.DataFrame()

  for _ , _row in dft.iterrows() :
    _df = build_df_for_each_id(_row[tid] , _row['r'])
    sdf = pd.concat([sdf , _df])

  ##
  ren = {
      'وضعیت جدید' : nsta ,
      'زمان'       : time ,
      'تاریخ'      : jdt ,
      }

  sdf = sdf.rename(columns = ren)
  ##
  sdf[jdt] = sdf[jdt].apply(make_zero_padded_jdate_ie_iso_fmt)
  ##
  sdf[jdtime] = sdf[jdt] + ' ' + sdf[time]
  ##
  sdf = sdf[[tid , row , jdtime , nsta]]
  ##
  sdf.drop_duplicates(inplace = True)
  ##
  rp_targ = GithubData(targ_rp_url)
  rp_targ.clone()
  ##
  dffp = rp_targ.data_fp
  ##
  sprq(sdf , dffp)
  ##
  cur_url = 'https://github.com/imahdimir/b-' + rp_targ.repo_name
  ##
  tokfp = '/Users/mahdi/Dropbox/tok.txt'
  tok = mu.get_tok_if_accessible(tokfp)
  ##
  msg = 'updated'
  msg += ' by: ' + cur_url

  rp_targ.commit_and_push(msg , user = rp_targ.user_name , token = tok)

  ##

  rp_targ.rmdir()
  rp_t2f.rmdir()


  ##

##


if __name__ == '__main__' :
  main()

##

##