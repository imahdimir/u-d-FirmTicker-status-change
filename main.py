"""

    """

import asyncio
import json
from functools import partial

import pandas as pd
from githubdata import GithubData
from mirutil.async_requests import get_reps_texts_async
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil.jdate import make_zero_padded_jdate_ie_iso_fmt
from mirutil.utils import ret_clusters_indices
from persiantools.jdatetime import JalaliDate


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    selff = gj['selff']
    src = gj['src']
    trg0 = gj['trg0']
    trg1 = gj['trg1']

gu = GDUrl()

class ColName :
    tic = 'Ticker'
    tid = 'TSETMC_ID'
    url = 'url'
    res = 'res'
    nst = 'NewStatus'
    t = 'Time'
    jd = 'JDate'
    row = 'Row'
    jdt = 'JDateTime'
    date = 'Date'
    iso = 'iso'
    dt = 'DateTime'
    ftic = 'FirmTicker'

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

    gd_src = GithubData(gu.src)
    gd_src.overwriting_clone()
    ##
    dft = gd_src.read_data()
    ##
    dft = dft[[c.tid]]
    dft = dft.drop_duplicates()
    ##
    dft = dft.dropna()
    ##
    dft[c.url] = cte.burl + dft[c.tid].astype(str)
    ##
    dft[c.res] = None
    df1 = dft.copy()
    ##
    while not df1.empty :
        msk = dft[c.res].isna()
        df1 = dft[msk]

        clus = ret_clusters_indices(df1)

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
    da = da.drop_duplicates()
    ##
    da[c.tid] = da[c.tid].astype('string')

    ##

    gd_trg0 = GithubData(gu.trg0)
    gd_trg0.overwriting_clone()
    ##
    dg = gd_trg0.read_data()
    ##

    dg = pd.concat([da , dg])
    ##
    dg = dg.drop_duplicates(subset = dg.columns.difference([c.row]))
    ##

    dgp = gd_trg0.data_fp
    sprq(dg , dgp)
    ##

    msg = 'data updated by: '
    msg += gu.selff
    ##

    gd_trg0.commit_and_push(msg)

    ##

    gd_trg0.overwriting_clone()
    ##
    dg = gd_trg0.read_data()
    ##
    c2s = [c.tid , c.jdt , c.row]
    dg = dg.sort_values(c2s , ascending = [False , False , True])
    ##
    msk = dg.duplicated([c.tid , c.jdt] , keep = False)
    df1 = dg[msk]
    len(df1)
    ##
    dg = dg.drop_duplicates([c.tid , c.jdt])
    ##
    dg = dg[[c.tid , c.jdt , c.nst]]

    ##

    dft = gd_src.read_data()
    ##
    dft[c.tid] = dft[c.tid].astype('string')
    ##
    dft = dft.set_index(c.tid)
    ##
    dg[c.ftic] = dg[c.tid].map(dft[c.ftic])
    ##
    dg = dg[[c.ftic , c.jdt , c.nst]]
    ##
    dg = dg.dropna()
    ##

    gd_trg = GithubData(gu.trg1)
    gd_trg.overwriting_clone()
    ##
    dgp = gd_trg.data_fp
    sprq(dg , dgp)
    ##

    msg = 'data updated by: '
    msg += gu.selff
    ##

    gd_trg.commit_and_push(msg)

    ##


    gd_src.rmdir()
    gd_trg0.rmdir()
    gd_trg.rmdir()


    ##

##
if __name__ == '__main__' :
    main()

##

##
