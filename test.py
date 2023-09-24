import pickle
from typing import NamedTuple
import pandas as pd
from urllib.parse import urlparse
from urllib.parse import parse_qs
import numpy as np
import requests
from requests.exceptions import ConnectTimeout, ChunkedEncodingError, ConnectionError, ReadTimeout, JSONDecodeError
from tqdm import tqdm
import sys
from multiprocessing import Pool as ThreadPool
from time import sleep as time_sleep, time
from pprint import pprint
from pathlib import Path
tqdm.pandas()

def separation_frame(input_df: pd.DataFrame, sep: int=5) -> list[pd.DataFrame]:
    out_list = list()
    chunk_size = input_df.shape[0] // int(100/sep)
    for index in range(0, input_df.shape[0], chunk_size):
        out_list.append(input_df.iloc[index:index+chunk_size].reset_index())
    return out_list

def convert_to_df(x):
    if not pd.isnull(x):
        tmp_df = pd.Series({
                                        'title': x.get('title', np.nan),
                                        'sku_id': x.get('uuid', np.nan),
                                        'dateStart': pd.to_datetime(x.get('dateStart', np.nan), unit='ms'),
                                        'dateEnd': pd.to_datetime(x.get('dateEnd', np.nan), unit='ms'),
                                        'price': x.get('priceData', {}).get('new', {}).get('from', np.nan),
                                        'price_from': x.get('priceData', {}).get('new', {}).get('to', np.nan),
                                        'price_to': x.get('priceData', {}).get('new', {}).get('value', np.nan),
                                        'discountPercent': x.get('discountPercent', np.nan),
                                        'quantity': x.get('quantity', np.nan),
                                        'quantityUnit': x.get('quantityUnit', np.nan),
                                        'segmentUuids': x.get('segmentUuids', np.nan),
        })
        
    else:
        tmp_df = pd.Series({
                                        'title': np.nan,
                                        'sku_id': np.nan,
                                        'dateStart': pd.to_datetime(np.nan, unit='ms'),
                                        'dateEnd': pd.to_datetime(np.nan, unit='ms'),
                                        'price': np.nan,
                                        'price_from': np.nan,
                                        'price_to': np.nan,
                                        'discountPercent': np.nan,
                                        'quantity': np.nan,
                                        'quantityUnit': np.nan,
                                        'segmentUuids': np.nan,
        })
    return tmp_df
# vec_convert_to_df = np.vectorize(convert_to_df)

def _works(x: pd.DataFrame):
    out = x['tmp'].apply(convert_to_df)
    return pd.concat((x, out), axis=1)


def run_convert_to_df(list_input, cores: int=16):
    with ThreadPool(cores) as pool:
        work_return = pool.map(func=_works, iterable=list_input)
    return work_return


def main():
    with open('data/out_test.pickle', 'rb') as file:
        out = pickle.load(file)
    
    # start = time()
    test = pd.Series(out, name='tmp').to_frame()
    test[['market', 'tmp']] = test['tmp'].apply(lambda x: pd.Series(x))
    test = pd.concat((test['market'].apply(lambda x: x), test['tmp']), axis=1)
    test = test.explode('tmp')
    test[['uuid', 'tmp']] = test['tmp'].apply(lambda x: pd.Series(x))
    test = test.explode('tmp').reset_index()

    # print(time() - start)

    
    for i in range(2, 50, 2):
        tt = separation_frame(test, i)
        for x in range(2, 30, 2):
            start = time()
            tmp = run_convert_to_df(tt, x)
            result = pd.concat(tmp, axis=0, ignore_index=True)
            print(f'persent: {i:2d}%, cores: {x:2d}, time: {(time() - start):7.2f}')
    # print(result.info())
    
if __name__ == '__main__':
    main()