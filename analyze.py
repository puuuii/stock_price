#%%
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import seaborn as sns
import matplotlib.pyplot as plt
import pickle
from fbprophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_error

sns.set_style('dark')

#%%
with open('./datas/basedata/basedata.pkl', "rb") as f:
    df = pickle.load(f)
df.head()

#%%
comture = df[df['code'] == 3844]
comture.head()

#%%
comture.isnull().sum()

#%%
comture['fluc'] = comture['end'] - comture['start']
comture['fluc'].plot(figsize=(15,5))

#%%
split_date = '2019/1/1'
train_comture = comture.loc[comture.index <= split_date]
test_comture = comture.loc[comture.index > split_date]

train_data = train_comture['fluc'].reset_index().rename(columns={'date':'ds', 'fluc':'y'})
test_data = test_comture.reset_index().rename(columns={'date':'ds'})['ds']

train_data.loc[(train_data['y'] > 200) & (train_data['y'] < -200)] = None

#%%
model = Prophet(changepoint_prior_scale=0.001, weekly_seasonality=False)
model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
model.fit(train_data)

#%%
future = model.make_future_dataframe(len(test_data))
predicted = model.predict(df=future)
fig = model.plot(predicted)

#%%
fig = model.plot_components(predicted)

#%%
