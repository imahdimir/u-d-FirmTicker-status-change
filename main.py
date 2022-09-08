"""

    """

import asyncio
from functools import partial

import nest_asyncio
import pandas as pd
from githubdata import GithubData
from mirutil import utils as mu
from mirutil.async_requests import get_reps_texts_async
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil.jdate import make_zero_padded_jdate_ie_iso_fmt


nest_asyncio.apply()

class GDUrl :
    trg = 'https://github.com/imahdimir/d-firm-status-change'
    t2f = 'https://github.com/imahdimir/d-TSETMC_ID-2-FirmTicker'
    cur = 'https://github.com/imahdimir/u-d-firm-status-change'

gu = GDUrl()

class ColName :
    btic = 'BaseTicker'
    tic = 'Ticker'
    tid = 'TSETMC_ID'
    url = 'url'
    res = 'res'
    nst = 'NewStatus'
    t = 'Time'
    jd = 'JDate'
    row = 'Row'
    jdt = 'JDateTime'

c = ColName()

class Constant :
    burl = 'http://tsetmc.com/Loader.aspx?Partree=15131L&i='

cte = Constant()

fu0 = partial(get_reps_texts_async , trust_env = True)

def build_df_for_each_id(id , resp_text) :
    dfs = pd.read_html(resp_text)
    assert len(dfs) == 1
    df = dfs[0]
    df[c.tid] = id
    df[c.row] = df.index
    return df

def main() :
    pass

    ##

    gd_t2f = GithubData(gu.t2f)
    gd_t2f.overwriting_clone()
    ##
    dft = gd_t2f.read_data()
    ##
    dft = dft[[c.tid]]
    dft.drop_duplicates(inplace = True)
    ##
    dft.dropna(inplace = True)
    ##
    dft[c.url] = cte.burl + dft[c.tid].astype(str)
    ##
    dft[c.res] = None
    df1 = dft.copy()
    ##
    while not df1.empty :
        msk = dft[c.res].isna()
        df1 = dft[msk]

        clus = mu.ret_clusters_indices(df1)

        for se in clus :
            print(se)
            si , ei = se
            inds = df1.iloc[si : ei].index

            urls = dft.loc[inds , c.url]

            out = asyncio.run(fu0(urls))

            dft.loc[inds , c.res] = out

            # break

        # break

    ##
    da = pd.DataFrame()

    for _ , ro in dft.iterrows() :
        _df = build_df_for_each_id(ro[c.tid] , ro[c.res])
        da = pd.concat([da , _df])

    ##
    ren = {
            'وضعیت جدید' : c.nst ,
            'زمان'       : c.t ,
            'تاریخ'      : c.jd ,
            }

    da = da.rename(columns = ren)

    ##
    da[c.jd] = da[c.jd].apply(make_zero_padded_jdate_ie_iso_fmt)
    ##
    da[c.jdt] = da[c.jd] + ' ' + da[c.t]
    ##
    da = da[[c.tid , c.row , c.jdt , c.nst]]
    ##
    da.drop_duplicates(inplace = True)
    ##
    da[c.tid] = da[c.tid].astype(str)

    ##

    gd_trg = GithubData(gu.trg)
    gd_trg.overwriting_clone()
    ##
    dpp = gd_trg.data_fp
    sprq(da , dpp)
    ##
    msg = 'data updated by: '
    msg += gu.cur
    ##

    gd_trg.commit_and_push(msg)

    ##

    gd_trg.rmdir()
    gd_t2f.rmdir()


    ##

##
if __name__ == '__main__' :
    main()

##

##
