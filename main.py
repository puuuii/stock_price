from download import download_data
from train import Trainer
from predict import Predictor


def main():
    # データをwebから取得
    download_data()


if __name__ == "__main__":
    main()