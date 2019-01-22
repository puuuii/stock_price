#%%
import zipfile
import shutil
import datetime
import os
import pandas as pd
import pickle
import numpy as np
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from urllib import request
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.decomposition import KernelPCA
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor

%matplotlib inline
%precision 3

sns.set()

#%%
# データディレクトリ初期化
zip_dir = "./datas/zips/"
csv_dir = "./datas/csvs/"

#%%
if os.path.exists(zip_dir):
    shutil.rmtree(zip_dir)
if os.path.exists(csv_dir):
    shutil.rmtree(csv_dir)

os.mkdir(zip_dir)
os.mkdir(csv_dir)

#%%
# zipファイルをwebから取得・保存するためのデータ群作成
url = "http://souba-data.com/k_data/"
get_length = 1500  # 何日前からのデータを取得するか
deltas = list(range(get_length))
today = datetime.date.today()
dates = [today - datetime.timedelta(days=delta) for delta in deltas]
years = [str(date.year) for date in dates]
monthes = [str(date.month).zfill(2) for date in dates]
days = [str(date.day).zfill(2) for date in dates]
urls = [url + year + "/" + year[-2:] + "_" + month + "/T" + year[-2:] + month + day + ".zip"
        for year, month, day in zip(years, monthes, days)]
pathes = [zip_dir + year[-2:] + month + day + ".zip"
          for year, month, day in zip(years, monthes, days)]

#%%
# zipデータダウンロード
for url, path in zip(urls, pathes):
    try:
        request.urlretrieve(url, path)
    except:
        pass

#%%
# zipファイルを展開
zip_objects = [zipfile.ZipFile(zip_dir+path, 'r') for path in os.listdir(zip_dir)]
_ = [zip.extractall(csv_dir) for zip in zip_objects]
_ = [zip.close() for zip in zip_objects]

#%%
# データ作成
csv_files = os.listdir(csv_dir)

list_df_data = [pd.read_csv(csv_dir+filename, encoding="shift-jis", header=None).drop([2, 3], axis=1)
                for filename in csv_files]
df_data = pd.concat(list_df_data, axis=0)
df_data.columns = ['date', 'code', 'start', 'top', 'bottom', 'end', 'dealings', 'place']
df_data['date'] = pd.to_datetime(df_data['date'])
df_data.set_index('date', inplace=True)
df_data.head()

#%%
# 証券コードをキーにしたdict化
dict_data = {key: df_data[df_data['code'] == key].drop('code', axis=1)
             for key in df_data['code'].unique()}

#%%
# データをpickle化
with open('./datas/dict_data.pkl', mode='wb') as f:
    pickle.dump(dict_data, f)

#%%
# pickleからデータ取得
pkl_path = './datas/dict_data.pkl'
with open(pkl_path, 'rb') as pkl:
    dict_data = pickle.load(pkl)

#%%
# 値が書き換えられないようディープコピー
df_target = dict_data[3906].copy()     # 任意の銘柄のデータ抽出
df_target.info()

#%%
df_target.sample(10)

#%%
df_target.isnull().sum()

#%%
# placeの欠損値は次の値で補完
df_target['place'].fillna(method='bfill', inplace=True)
df_target.isnull().sum()

#%%
# 日中変動を特徴量に追加
df_target['diff'] = df_target['end'] - df_target['start']
df_target.sample(30)

#%%
# トレンド、周期変動、残差に分解
N_freq = 30
res = sm.tsa.seasonal_decompose(df_target['end'], freq=N_freq, two_sided=False)
df_target['end_trend'] = res.trend
df_target['end_seasonal'] = res.seasonal
df_target['end_residual'] = res.resid

df_target.drop(index=df_target.index.values[:N_freq], inplace=True)
df_target.head(20)

#%%
df_target['end'].plot()
df_target['end_trend'].plot()
df_target['end_seasonal'].plot()
df_target['end_residual'].plot()
plt.legend()

#%%
df_target['diff'].tail(100).plot()
df_target['end_trend'].tail(100).plot()
df_target['end_seasonal'].tail(100).plot()
df_target['end_residual'].tail(100).plot()
plt.legend()

#%%
df_target.tail()

#%%
# 使用する列を絞り込む
tmp = df_target['diff'].values[1:]
df_target = df_target[['diff', 'end_trend', 'end_seasonal', 'end_residual', 'dealings']].iloc[:-1]
df_target['y'] = tmp

df.shape

#%%
df_target.tail(10)

#%%
sns.pairplot(df_target)

#%%
# numpy化とともに説明変数と目的変数に
X = df_target.drop('y', axis=1).values
y = df_target['y'].values

print(X[:10])
print(y[:10])

#%%
# 標準化
sc = StandardScaler()
X = sc.fit_transform(X)

X[0:10]

#%%
# データ検証用分割
X_train, X_valid, y_train, y_valid = \
    train_test_split(X, y, test_size=0.2, random_state=0)

#%%
plt.plot(y_train)

#%%
# XGBRegressor
xgbr = XGBRegressor(random_state=0)
param_max_depth = [1, 2, 3, 4, 5]
param_learning_rate = [0.1, 0.2, 0.3]
param_subsample = [0.8, 0.9, 1.0]
param_colsample_bytree = [0.1, 0.2, 0.3]
param_grid = [{'max_depth': param_max_depth,
               'learning_rate': param_learning_rate,
               'subsample': param_subsample,
               'colsample_bytree': param_colsample_bytree}]
gs = GridSearchCV(estimator=xgbr, param_grid=param_grid, scoring='neg_mean_absolute_error',
                  cv=2, return_train_score=False, n_jobs=-1)
gs.fit(X_train, y_train)

#%%
# 結果表示
clf_pca_xgbr = gs.best_estimator_
pred = clf_pca_xgbr.predict(X_valid)

print(gs.best_score_, clf_pca_xgbr.score(X_valid, y_valid))
print(gs.best_params_)

plt.plot(pred, pred - y_valid, 'o')
plt.hlines(y=0, xmin=min(pred), xmax=max(pred))
plt.title('residual')

#%%
