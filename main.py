from download import download_data
from basedata import make_basedata
from train import Trainer
from predict import Predictor


N_PERIOD = 3000   # データ取得期間（日）


def main():
    # データをwebから取得
    print('start: download_data')
    download_data(N_PERIOD)

    # web取得したデータを縦持ちのDataFrameにしてpickle化
    print('start: make_basedata')
    make_basedata()


if __name__ == "__main__":
    main()