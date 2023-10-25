import pandas as pd
import numpy as np
import requests
from requests.exceptions import ConnectTimeout, ChunkedEncodingError, ConnectionError, ReadTimeout, JSONDecodeError
from tqdm import tqdm
# from tqdm.notebook import tqdm
import sys
from multiprocessing.dummy import Pool as ThreadPool
# from multiprocessing import Pool
from time import time_ns, time, sleep as time_sleep
from pathlib import Path
from datetime import datetime
from pytz import timezone
from itertools import chain
from pandarallel import pandarallel

tqdm.pandas()
requests.packages.urllib3.disable_warnings()
pandarallel.initialize(verbose=0)


seg_df = pd.read_csv('data/segments.zip')
ret_df = pd.read_csv('data/retailers.zip')
loc_df = pd.read_csv('data/located_list.zip')
seg_id_df = pd.read_csv('data/segments_id.zip')[['uuid', 'name']].drop_duplicates(ignore_index=True)



def _save_df_to_zip(df_: pd.DataFrame, archive_name: str = 'archive', folder: str='data', replace: bool=False) -> None:
    # Путь к файлу
    file_path = Path(folder).joinpath(archive_name + '.zip')
    Path(folder).mkdir(exist_ok=True)
    # Проверяем, существует ли файл
    if file_path.exists() and not replace:
        # Получаем время создания файла
        time = datetime.fromtimestamp(file_path.lstat().st_atime).strftime('%Y-%m-%d %H-%M')

        # Создаем новое имя файла с добавлением времени Unix
        new_file_name = file_path.stem + " " + str(time) + file_path.suffix

        # Создаем новый путь для переименованного файла
        new_file_path = file_path.with_name(new_file_name)
        # Переименовываем файл
        file_path.rename(new_file_path)

# to csv
    compression_opts = dict(method='zip', archive_name=f'{archive_name}.csv')
    df_.to_csv(f'{folder}/{archive_name}.zip', index=False, compression=compression_opts, encoding='utf-8')
    
def separation_frame(input_df: pd.DataFrame, sep: int=5) -> list[pd.DataFrame]:
    """
        Функция для дробления DataFrame, что бы потом паралельно обрабатывать их паралельно.
        sep: Сколько процентов будет занимать сегмент.
    """
    out_list = list()
    chunk_size = input_df.shape[0] // int(100/sep)
    for index in range(0, input_df.shape[0], chunk_size):
        out_list.append(input_df.iloc[index:index+chunk_size].reset_index())
    return out_list



def get_data(pool1_input: tuple[tqdm, int, tuple[int, str]]):
    global loc_df, seg_df
    
    def get_response(url, params, headers):
        for _ in range(5):
            try:
                respons = requests.get(url, params=params, headers=headers, timeout=(10, 30), verify=False)
            except ConnectTimeout:
                time_sleep(1)
                continue
            except ChunkedEncodingError:
                time_sleep(1)
                continue
            except ConnectionError:
                time_sleep(1)
                continue
            except ReadTimeout:
                time_sleep(1)
                continue
            except Exception:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print(exc_type.__name__)
                return None
            else:    
                try:
                    return respons.json().get('items', None)
                except JSONDecodeError:
                    return None
        else:
            return None    


    
    def get_su2_su3(pool2_input: tuple):
        global seg_df
        
        url, params, headers, su2 = pool2_input
        params['segmentUuid'] = [su2]

        respons_2 = get_response(url, params, headers)
                                
        if respons_2:
            tmp_list_02 = list()
            try:
                su3_list = list(set([(x.get('segmentUuids', []))[-1] for x in respons_2]))
                len_sku_list = len(su3_list)
            except Exception:
                su3_list = None
            
            if not su3_list or (len_sku_list>550):
                su3_list = seg_df[(seg_df['uuid'] == su1) & (seg_df['uuid level 02'] == su2)]['uuid level 03'].unique()
            
            for su3 in su3_list:
                params['segmentUuid'] = [su3]
                respons_3 = get_response(url, params, headers)

                if respons_3:
                    tmp_list_02.append((su3, respons_3))
            
            if tmp_list_02:
                return tmp_list_02
            else:
                return [(su2, respons_2)]
        return []

    
        
    pbar, streams, (index, item) = pool1_input
    
    out_list = list()
    
    res = loc_df[loc_df['slug'] == item.sity].min()
    headers = {
                'x-locality-geoid': str(res.geoId),
                'x-position-latitude': f'{res.lat:.5f}',
                'x-position-longitude': f'{res.lng:.5f}'
                }
        
    url = f'https://search.edadeal.io/api/v4/retailer/{item.data_uuid}/items'

    params = {  'addContent': ['true'],
                'checkAdult': ['true'],
                'excludeSegmentSlug': ['alcohol', 'pt_alcool', 'en_alcohol', 'es_alcohol', 'tr_alkol'],
                'groupBy': ['sku_or_meta'],
                'numdoc': ['599'],
                'page': ['0'],
                'segmentUuid': []
             }

    # for su1 in tqdm(seg_df['uuid'].unique()[:], desc=f'{item.sity} / {item.market}', leave=False):
    for su1 in seg_df['uuid'].unique()[:]:
        params['segmentUuid'] = [su1]
        
        respons_1 = get_response(url, params, headers)
        
        if respons_1:
            
            try:
                su2_list = list(set([(x.get('segmentUuids', []))[-1] for x in respons_1]))
                len_sku_list = len(su2_list)
            except Exception:
                su2_list = None
            
            if not su2_list or (len_sku_list>550):
                su2_list = seg_df[seg_df['uuid'] == su1]['uuid level 02'].unique()
            
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            with ThreadPool(streams) as pool2:
                su2_len = len(su2_list)
                input_list = list(zip([url]*su2_len, [params]*su2_len, [headers]*su2_len, su2_list))
                work_return = pool2.map(get_su2_su3, input_list)    
                tmp_list_01 = chain.from_iterable(work_return)
                
            if tmp_list_01:
                out_list.extend(tmp_list_01)
            else:
                out_list.append((su1, respons_1))
        else:
            continue
            
    pbar.update(1)
    return (item, out_list)

def run_get_price(list_input, streams1:int=4, streams2:int=4):
    # Первый нюанс!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # Функция деления на потоки.
    # где cores - это количество потоков
    with tqdm(total=len(list_input), desc='Download', leave=False) as pbar:
        list_input_len = len(list_input)
        list_to_pool = list(zip([pbar]*list_input_len, [streams2]*list_input_len, list_input))
        # with Pool(streams1) as pool1:
        with ThreadPool(streams1) as pool1:
            work_return = pool1.map(get_data, list_to_pool)
    return work_return


def convert_data_to_df(input_list: list, seg_id_df) -> pd.DataFrame:
    import pandas as pd
    import numpy as np
    def convert_to_df(x: dict[str,dict[str,dict]]):
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
        
    start = int(time())
    tmp_df = pd.Series(input_list, name='tmp').to_frame()
    tmp_df[['market', 'tmp']] = tmp_df['tmp'].apply(lambda x: pd.Series(x))
    tmp_df = pd.concat((tmp_df['market'].apply(lambda x: x), tmp_df['tmp']), axis=1)
    tmp_df['date'] = pd.to_datetime(tmp_df['date'])
    tmp_df = tmp_df.explode('tmp', ignore_index=True)
    tmp_df[['uuid', 'tmp']] = tmp_df['tmp'].apply(lambda x: pd.Series(x))
    tmp_df = tmp_df.explode('tmp', ignore_index=True)
    # Второй нюанс!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # Для ускорения работы обработки данных использую паралельное вычисление,
    # Закоментирован вариант без паралельной обработки.
    # tmp_df = pd.concat((tmp_df, tmp_df['tmp'].apply(convert_to_df)), axis=1)
    convert_data_before_convert = int(time()) - start
    
    start = int(time())
    tmp_df = pd.concat((tmp_df, tmp_df['tmp'].parallel_apply(convert_to_df)), axis=1)
    convert_data_convert = int(time()) - start
    
    start = int(time())
    tmp_df[['su1', 'su2', 'su3']] = tmp_df['segmentUuids'].apply(lambda x: pd.Series(x))
    tmp_df['title'] = tmp_df['title'].str.replace('\n', ' ')
    
    tmp_df = tmp_df[[
                    # 'index',
                    'date',
                    'sity',
                    'market',
                    'discounts',
                    'data_uuid',
                    'href',
                    # 'tmp',
                    # 'uuid',
                    'su1',
                    'su2',
                    'su3',
                    'title',
                    'sku_id',
                    'dateStart',
                    'dateEnd',
                    'price',
                    'price_from',
                    'price_to',
                    'discountPercent',
                    'quantity',
                    'quantityUnit',
                    # 'segmentUuids',
                    ]]
    
    tmp_df = tmp_df.drop_duplicates(ignore_index=True)
    
    tmp_df['su1'] = tmp_df['su1'].to_frame().merge(
                                                    right=seg_id_df,
                                                    how='left',
                                                    left_on='su1',
                                                    right_on='uuid'
                                                    )['name']

    tmp_df['su2'] = tmp_df['su2'].to_frame().merge(
                                                    right=seg_id_df,
                                                    how='left',
                                                    left_on='su2',
                                                    right_on='uuid'
                                                    )['name']

    tmp_df['su3'] = tmp_df['su3'].to_frame().merge(
                                                    right=seg_id_df,
                                                    how='left',
                                                    left_on='su3',
                                                    right_on='uuid'
                                                    )['name']
    
    tmp_df['sity'] = tmp_df['sity'].to_frame().merge(
                                                right=loc_df,
                                                how='left',
                                                left_on='sity',
                                                right_on='slug'
                                                )['localityName']
    
    convert_data_after_convert = int(time()) - start
    
    time_df = pd.Series([convert_data_before_convert, convert_data_convert, convert_data_after_convert],
                        index=['convert_data_before_convert', 'convert_data_convert', 'convert_data_after_convert']
                        )
    # return tmp_df
    return (tmp_df, time_df)

def main():
    test_list = []
    for i in [2,4,6,8,10,12,14,16,26,32,64,96,128]:
        for j in [2,4,6,8,10,12,14,16,26,32,64,96,128]:    
            # start_programm = datetime.strftime(datetime.now(timezone('Europe/Moscow')), '%Y-%b-%d %H:%M:%S')
            start_programm = int(time())
            
            
            control_time = list()

            frame_list = separation_frame(ret_df, 2)[:1]
            with tqdm(total=len(frame_list), desc='Total') as pbar:
                for index, df in enumerate(frame_list):
                    
                    # start = int(time())
                    out = run_get_price(list(df.iterrows()), streams1=i, streams2=j)
                    # get_price_time = int(time()) - start
                    
                    # start = int(time())
                    # out, time_df = convert_data_to_df(out, seg_id_df)
                    # convert_data_time = int(time()) - start
                    
                    # start = int(time())
                    # _save_df_to_zip(out, f'result {index:04d}')
                    # save_time = int(time()) - start
                    
                    # tmp_time_series = pd.Series([get_price_time, convert_data_time, save_time], index=['get_price_time', 'convert_data_time', 'save_time'])
                    
                    # control_time.append(pd.concat((tmp_time_series, time_df)))
                    pbar.update(1)
            
            # control_time = pd.DataFrame(control_time)
            # control_time['sum'] = control_time['get_price_time'] + control_time['convert_data_time'] + control_time['save_time']
            # control_time['get_price_time_d'] = round(control_time['get_price_time']/control_time['sum']*100).astype('UInt8')
            # control_time['convert_data_time_d'] = round(control_time['convert_data_time']/control_time['sum']*100).astype('UInt8')
            # control_time['save_time_d'] = round(control_time['save_time']/control_time['sum']*100).astype('UInt8')
            # print(control_time)
            
            end_programm = int(time()) - start_programm
            test_list.append(pd.Series([i,j,end_programm], index=['streams1', 'streams2', 'time']))
    
    control_time = pd.DataFrame(test_list)
    control_time.to_parquet('data/control_time_parquet.gzip', engine='pyarrow', compression='gzip')    
    
    
if __name__ == '__main__':
    main()