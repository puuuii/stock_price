import quandl
import zipfile
import shutil
import datetime
import os
import pandas as pd
import requests
import glob


GET_LENGTH = 100       # 何日分さかのぼってデータ取得するか
quandl.ApiConfig.api_key = '5DqEjiAZzgnfe6bWDJr5'


def download_data():
   _get_datas(GET_LENGTH)


def _get_datas(get_length):
    # ディレクトリ&ファイル初期化
    datas_dir = 'datas/'
    zip_dir = datas_dir + 'zips/'
    csv_dir = datas_dir + 'csvs/'
    try:
        shutil.rmtree(datas_dir)
    except FileNotFoundError:
        pass
    os.mkdir(datas_dir)
    os.mkdir(zip_dir)
    os.mkdir(csv_dir)

    # zipファイルをwebから取得・保存
    url = "http://souba-data.com/k_data/"
    today = datetime.date.today()
    urls = [_make_url(url, today, delta) for delta in range(get_length)]
    _ = [_make_zip(url, zip_dir) for url in urls]
    _ = [_extract_all(zip_path, csv_dir) for zip_path in glob.glob(zip_dir + '/*')]
    

def _make_url(url, today, delta):
    date = today - datetime.timedelta(days=delta)
    year = str(date.year)
    month = str(date.month).zfill(2)
    day = str(date.day).zfill(2)
    url = url + year + "/" + year[-2:] + "_" + month + "/T" + year[-2:] + month + day + ".zip"

    return url


def _make_zip(url, zip_dir):
    filename = zip_dir + url.split('/')[-1]
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)


def _extract_all(zip_path, csv_dir):
    with zipfile.ZipFile(zip_path) as zip:
        zip.extractall(csv_dir)


if __name__ == "__main__":
    download_data()