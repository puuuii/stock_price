import pandas as pd
import pickle
import shutil
import os


def make_basedata():
    # 縦持ちのDataFrame作成
    basedata = _make_data_dict()

    # pickle化
    _pickle_data(basedata)


def _make_data_dict():
    # 読み込みパス作成
    csv_dir = './datas/csvs/'
    csv_pathes = [(csv_dir + path) for path in os.listdir(csv_dir)]

    # DataFrame作成
    basedict = _csv_to_df(csv_pathes)

    return basedict


def _csv_to_df(csv_pathes):
    # 全csvデータから縦に結合したDataFrameを作成
    dfs = [pd.read_csv(path, encoding="shift-jis", header=None, index_col=0) for path in csv_pathes]
    df = pd.concat(dfs, axis=0, sort=True)
    df = df.iloc[:, [0, 2, 3, 4, 5, 6, 7, 8]]
    df.columns = ['code', 'name', 'start', 'top', 'bottom', 'end', 'value', 'place']
    print(df.head())
    df.index.name = 'date'

    df['name'] = df['name'].apply(lambda x: str(x).split(' ')[-1])

    return df


def _pickle_data(dict_data):
    # 書き出しディレクトリ初期化
    datas_dir = 'datas/'
    basedata_dir = datas_dir + 'basedata/'
    try:
        shutil.rmtree(basedata_dir)
    except FileNotFoundError:
        pass
    os.mkdir(basedata_dir)

    # pickle化
    filepath = basedata_dir + 'basedata.pkl' 
    with open(filepath, 'wb') as f:
        pickle.dump(dict_data, f, protocol=4)


if __name__ == "__main__":
    make_basedata()