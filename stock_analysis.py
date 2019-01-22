from urllib import request
import zipfile
import shutil
import datetime
import os
import pandas as pd
import pickle
import numpy as np
from numpy import array, concatenate


def main():
    """
    メイン処理
    """

    # webからデータ取得（実行はフラグで手動管理）
    do_get_datas = False
    get_length = 1500       # 何日分さかのぼってデータ取得するか
    if do_get_datas:
        get_datas(get_length)

    # {証券コード: 銘柄名}辞書作成
    security_code_dict = make_security_code_dict()

    # 分析用株価データ作成（実行はフラグで手動管理）
    make_data = False
    stock_data = {}
    if make_data:
        stock_data = make_stock_data(security_code_dict)

    # 分析用その他データ作成（米ドル円、NYダウ）
    other_data = make_other_data()

    # 保存したpklファイルからデータ取得（実行はフラグで手動管理）
    do_get_from_pkl = False
    if do_get_from_pkl:
        pkl_path = 'data.pkl'
        with open(pkl_path, 'rb') as pkl:
            stock_data = pickle.load(pkl)

    # 分析開始
    # results = {code: analyze(stock_data[code], other_data) for code in security_code_dict.keys()}


def get_datas(get_length):
    """
    web上から株価データ取得処理
    """

    # ファイル削除
    zip_dir = "datas/zips/"
    csv_dir = "datas/csvs/"
    zipfiles = os.listdir(zip_dir)
    for filename in zipfiles:
        filepath = zip_dir + filename
        os.remove(filepath)
    csvfiles = os.listdir(csv_dir)
    for filename in csvfiles:
        filepath = csv_dir + filename
        os.remove(filepath)

    # zipファイルをwebから取得・保存
    url = "http://souba-data.com/k_data/"
    today = datetime.date.today()
    for delta in range(get_length):
        get_day = today - datetime.timedelta(days=delta)
        year = str(get_day.year)
        month = str(get_day.month).zfill(2)
        day = str(get_day.day).zfill(2)
        get_url = url + year + "/" + year[-2:] + "_" + month + "/T" + year[-2:] + month + day + ".zip"
        file_path = zip_dir + year[-2:] + month + day + ".zip"
        try:
            request.urlretrieve(get_url, file_path)
            extract_csv(file_path, csv_dir)
        except:
            pass


def extract_csv(file_path, csv_dir):
    """
    zipファイルを解凍・中のcsvファイルを移動
    --------------------------------------
    :param file_path: 解凍zipファイルパス
    :param csv_dir:   書き出しディレクトリ
    """

    with zipfile.ZipFile(file_path, 'r') as zf:
        for f in zf.namelist():
            if not os.path.basename(f):
                os.mkdir(f)
            else:
                with open(f, 'wb') as uzf:
                    uzf.write(zf.read(f))

            # ファイル移動に失敗するとファイル削除
            try:
                shutil.move(f, csv_dir)
            except:
                os.remove(f)


def make_security_code_dict():
    """
    {証券コード: 銘柄名}辞書作成
    ---------------------------
    :return: {証券コード: 銘柄名}辞書
    """

    url = "http://kabusapo.com/dl-file/dl-stocklist.php"
    df = pd.read_csv(url, header=None)
    df = df.iloc[1:, 0:2]

    return {int(code): name for code, name in zip(df[0], df[1])}


def make_stock_data(security_code_dict):
    """
    株価予測に用いるデータ作成
    :return: 株価予測データ
    """

    # {証券コード: 株価データ表}の返却データ初期化
    empty_array = np.empty([0,6])
    stock_data = {key: empty_array  for key in security_code_dict.keys()}

    # データ作成
    print("-- start data extracting --")
    csvdir_path = "datas/csvs/"
    csv_files = os.listdir(csvdir_path)
    total_progress = len(csv_files)
    current_progress = 0
    # csvファイルを1つずつ読み込み、1行ずつ取り出してデータフレームを作成
    for filename in csv_files:
        # 進捗出力
        remain_progress = total_progress - current_progress
        o_str = ["*" for i in range(current_progress)]
        o_str += ["­" for i in range(remain_progress)]
        print(str(current_progress) + "/" + str(total_progress), ''.join(o_str))
        current_progress += 1

        # データフレーム作成
        arr = array(pd.read_csv(csvdir_path + filename, encoding="shift-jis", header=None))
        for row in arr:
            if row[1] in stock_data:
                stock_data[row[1]] = concatenate([stock_data[row[1]], [row[[False, False, False, False, True, True, True, True, True, True]]]], axis=0)

    stock_data = {key: pd.DataFrame(value, columns=["start", "end", "under", "top", "turnover", "place"]) for key, value in stock_data.items()}
    print("-- end data extracting --")

    # データをpickle
    with open('data.pkl', mode='wb') as f:
        pickle.dump(stock_data, f)

    return stock_data


def make_other_data():
    """
    分析用その他データ作成
    :return: [米ドル円, NYダウ]円データ（データフレーム）
    """

    # webから為替データ取得
    fx_data = pd.read_csv("https://www.mizuhobank.co.jp/rate_fee/market/csv/quote.csv", encoding="shift-jis", header=None)
    fx_data = fx_data.iloc[3:,0:2]  # 日付, 米ドルの2列抽出

    # webからNYダウデータ取得
    dau_data = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?chart_type=line&recession_bars=on&log_scales=&bgcolor=%23e1e9f0&graph_bgcolor=%23ffffff&fo=Open+Sans&ts=12&tts=12&txtcolor=%23444444&show_legend=yes&show_axis_titles=yes&drp=0&cosd=2013-03-29&coed=2018-03-28&height=450&stacking=&range=Custom&mode=fred&id=DJIA&transformation=lin&nd=2008-03-28&ost=-99999&oet=99999&lsv=&lev=&mma=0&fml=a&fgst=lin&fgsnd=2009-06-01&fq=Daily&fam=avg&vintage_date=&revision_date=&line_color=%234572a7&line_style=solid&lw=2&scale=left&mark_type=none&mw=2&width=1168", encoding="shift-jis")
    # https://nikkeiyosoku.com/dtwexm/csv
    dau_data = [(date.replace('-', '/'), data) for date, data in zip(dau_data.iloc[:, 0], dau_data.iloc[:, 1])]

    # 株データcsvのファイル名を為替データの日付に形式に合わせる
    csvdir_path = "datas/csvs/"
    csv_files = os.listdir(csvdir_path)
    filenames = [format_csvfile_string(filename) for filename in csv_files]

    # 各種データを取得できた日の為替データのみ抽出（欠損値は前営業日の値で補完）
    fx_data = [float(data) for date, data in zip(fx_data.iloc[:,0], fx_data.iloc[:,1]) if str(date) in filenames]
    dau_data = [[dau_data[i][0], dau_data[i][1]] if dau_data[i][1] != '.' else [dau_data[i][0], dau_data[i-1][1]] for i in range(len(dau_data))]
    dau_data = [float(data[1]) for data in dau_data if format_date_string(data[0]) in filenames]

    df = pd.DataFrame([fx_data, dau_data], index=["doller/yen", "dau"]).T

    return df


def format_csvfile_string(filename):
    """
    入力されるファイル名から日付文字列の体裁を整えて返す
    ---------------------
    :param filename: 入力ファイル名
    :return: 体裁整え完了後文字列
    """

    filename = filename[1:7]    # 日付部のみ抽出
    filename = "20" + filename[:2] + "/" + str(int(filename[2:4])) + "/" + str(int(filename[-2:]))

    return filename


def format_date_string(datestr):
    """
    入力される日付文字列の0埋め解除
    -----------------------------
    :param datestr: 日付文字列
    :return: 0埋め解除後日付文字列
    """

    year = datestr[:4]
    month = datestr[5:7]
    day = datestr[-2:]

    retstr = str(int(year)) + "/" + str(int(month)) + "/" + str(int(day))

    return retstr


def analyze(df, other_data):
    """
    株価予測分析実行
    ---------------
    :param df: 特定銘柄のデータフレーム型データ
    :param fx_data: 米ドル為替データ
    :return: 予測値上がり率（float）
    """

    # 東証1部だけ対象
    if df.loc[0, 'place'] != "東証１部":
        return None

    # 説明変数取得
    X = make_explanatory_var(df, other_data)

    # 目的変数取得
    Y = make_criterion_var(df)

    exit()

    return 1.0


def make_explanatory_var(df, other_data):
    """
    説明変数作成
    -----------
    :param df: 株価データデータフレーム
    :param other_data: 米ドル、NYダウ
    :return: 説明変数データフレーム
    """

    nparray = np.array(pd.concat([df.iloc[:,:-1], other_data], axis=1))

    means = []
    tops = []
    unders = []
    turnovers = []
    dollyens = []
    daus = []
    five_delta = 0
    top_under_list = []
    five_turnover = 0
    five_dollyen = 0
    five_dau = 0
    five_counter = 0
    for row in nparray:
        five_delta += row[1] - row[0]       # 日の値動き
        top_under_list.append(row[3])       # 高値
        top_under_list.append(row[2])       # 安値
        five_turnover += row[4]             # 出来高
        five_dollyen += row[5]              # 米ドル円
        five_dau += row[6]                  # NYダウ
        five_counter += 1
        # 5日分の差分がたまったら各種値保存
        if five_counter % 5 == 0:
            means.append(five_delta/5)
            tops.append(max(top_under_list))
            unders.append(min(top_under_list))
            turnovers.append(five_turnover/5)
            dollyens.append(five_dollyen/5)
            daus.append(five_dau/5)
            five_delta = 0
            top_under_list = []
            five_turnover = 0
            five_dollyen = 0
            five_dau = 0
            five_counter = 0

    explanatory_df = pd.DataFrame([means, tops, unders, turnovers, dollyens, daus]).T
    # print(explanatory_df)

    return explanatory_df


def make_five_day_tops_unders(df):
    """
    5日最高値・最安値リスト作成
    ------------------
    :param df: 株価データデータフレーム
    :return: 5日最高値・最安値リスト
    """

    tops = []
    unders = []
    tmp_list = []
    five_counter = 0
    for top, under in zip(df.loc[:, 'top'], df.loc[:, 'under']):
        tmp_list.append(top)
        tmp_list.append(under)
        five_counter += 1

        # 5日分の値がたまったら最高値・最安値を求める
        if five_counter % 5 == 0:
            tops.append(max(tmp_list))
            unders.append(min(tmp_list))
            tmp_list = []
            five_counter = 0

    return tops, unders


def make_criterion_var(df):
    """
    目的変数作成
    ------------
    :param df: 株価データデータフレーム
    :return: 目的変数データフレーム
    """

    criterion_df = []

    return criterion_df


if __name__ == '__main__':
    main()