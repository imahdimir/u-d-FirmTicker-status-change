##

"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil import funcs as mf
from aiohttp import ClientSession
import asyncio
import nest_asyncio


nest_asyncio.apply()

stch_rp_url = 'https://github.com/imahdimir/d-Status-changes'
cur_rp_url = 'https://github.com/imahdimir/get-Status-changes-TSETMC'

btic = 'BaseTicker'
tic = 'Ticker'
tid = 'TSETMC_ID'
url = 'url'
nsta = 'NewStatus'
time = 'Time'
jdt = 'JDate'

status_chng_burl = 'http://tsetmc.com/Loader.aspx?Partree=15131L&i='

def status_change_url(tsetmc_id) :
  return f'{status_chng_burl}{tsetmc_id}'

async def get_resp_text(url , trust_env = True) :
  async with ClientSession(trust_env = trust_env) as session :
    async with session.get(url) as resp :
      return await resp.text()

async def get_resps_of_urls(urls , trust_env = True) :
  fu = get_resp_text
  tasks = [asyncio.create_task(fu(x , trust_env = trust_env)) for x in urls]

  return await asyncio.gather(*tasks)

def build_df_for_each_id(id , resp_text) :
  dfs = pd.read_html(resp_text)

  assert len(dfs) == 1

  df = dfs[0]
  df[tid] = id

  return df

def main() :
  pass

  ##
  idf = pd.read_parquet('in.prq')
  ##
  idf = idf.dropna()
  ##
  idf[url] = idf[tid].apply(status_change_url)
  ##
  cls = mf.return_clusters_indices(idf[url])
  ##
  for se in cls :
    print(se)

    urls = idf.loc[se[0] : se[1] , url]

    out = asyncio.run(get_resps_of_urls(urls))
    idf.loc[se[0] : se[1] , 't'] = out

    # break

  ##
  idf.to_parquet('temp0.prq' , index = False)

  ##
  sdf = pd.DataFrame()

  for _ , row in idf.iterrows() :
    _df = build_df_for_each_id(row[tid] , row['t'])
    sdf = pd.concat([sdf , _df])

  ##
  ren = {
      'وضعیت جدید' : nsta ,
      'زمان'       : time ,
      'تاریخ'      : jdt ,
      }

  sdf = sdf.rename(columns = ren)
  ##
  sdf = sdf[[tid , jdt , time , nsta]]
  ##
  sdf = sdf.drop_duplicates()
  ##
  sdf[jdt] = sdf[jdt].apply(mf.make_zero_padded_jdate)
  ##
  sdf.to_parquet('out.prq' , index = False)

  ##
  stch_rp = GithubData(stch_rp_url)
  stch_rp.clone()

  ##
  pdfpn = stch_rp.data_filepath
  pdf = pd.read_parquet(pdfpn)
  ##
  pdf = pd.concat([pdf , sdf])
  ##
  pdf = pdf.drop_duplicates()
  ##
  pdf.to_parquet(pdfpn , index = False)
  ##
  msg = 'init'
  msg += ' by: ' + cur_rp_url

  stch_rp.commit_push(msg)

  ##

  stch_rp.rmdir()


  ##


##