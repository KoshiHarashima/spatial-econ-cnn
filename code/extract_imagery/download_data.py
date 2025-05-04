import os
import sys
import tables
import logging

import numpy as np
import pandas as pd
import tensorflow as tf
np.random.seed(13298)
tf.config.threading.set_inter_op_parallelism_threads(1)

tf.compat.v1.disable_eager_execution()

from google_drive_utils import GDFolderDownloader
from params import *

# ログ設定：実行中のスクリプト名をLogger名に使っている
LOG = logging.getLogger(os.path.basename(__file__))
ch = logging.StreamHandler()
log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
ch.setFormatter(logging.Formatter(log_fmt))
ch.setLevel(logging.INFO)
LOG.addHandler(ch)
LOG.setLevel(logging.INFO)

# プロジェクトのルートディレクトリを環境変数から取得（なければ相対パスを使用）
ROOT = os.environ.get("CNN_PROJECT_ROOT", "../")

# コマンドライン引数からモード（large, small, mw）を取得し、バリデーション
mode = sys.argv[1]
if mode not in ["large", "small", "mw"]:
    raise Exception("Mode must be 'large','small', or 'mw'")

# HDF5出力ファイルのパスを設定
path = f"{ROOT}/data/{mode}_images_all_years_raw.h5"


# モードによってGoogle DriveのディレクトリID、画像サイズ、チャンネル名を設定
if mode == "small":
    root_dir_id = "1d1Fw4nuM_9a8xAguehLFW7UmsZVk8dt-"
    IMG_ROWS_RAW = 54
    IMG_COLS_RAW = 54
    CHANNEL_NAMES = CHANNEL_NAMES_SMALL
elif mode == "large":
    root_dir_id = "1TyZ9FEFr0ySaPxXHLyIUet6jEB3NEwbe"
    IMG_ROWS_RAW = 94
    IMG_COLS_RAW = 94
    CHANNEL_NAMES = CHANNEL_NAMES_LARGE
elif mode == "mw":
    root_dir_id = "1qifSHdP_UOQKTvc2vzUgAIy3yq6id_HR"
    IMG_ROWS_RAW = 108
    IMG_COLS_RAW = 108
    CHANNEL_NAMES = CHANNEL_NAMES_MW

# チャンネル数を含めた画像の形状を定義
IMG_SHAPE = (IMG_ROWS_RAW, IMG_COLS_RAW, len(CHANNEL_NAMES))

# 年代のリストと、HDF5に保存するためのデータ構造（画像＋位置情報）を定義
if mode in ["large", "small"]:
    YEARS = list(range(0,20))
    class IMGData(tables.IsDescription):
        # 各年の画像データをフィールドとして定義
        img0 = tables.Float32Col(shape=IMG_SHAPE)
        img1 = tables.Float32Col(shape=IMG_SHAPE)
        img2 = tables.Float32Col(shape=IMG_SHAPE)
        img3 = tables.Float32Col(shape=IMG_SHAPE)
        img4 = tables.Float32Col(shape=IMG_SHAPE)
        img5 = tables.Float32Col(shape=IMG_SHAPE)
        img6 = tables.Float32Col(shape=IMG_SHAPE)
        img7 = tables.Float32Col(shape=IMG_SHAPE)
        img8 = tables.Float32Col(shape=IMG_SHAPE)
        img9 = tables.Float32Col(shape=IMG_SHAPE)
        img10 = tables.Float32Col(shape=IMG_SHAPE)
        img11 = tables.Float32Col(shape=IMG_SHAPE)
        img12 = tables.Float32Col(shape=IMG_SHAPE)
        img13 = tables.Float32Col(shape=IMG_SHAPE)
        img14 = tables.Float32Col(shape=IMG_SHAPE)
        img15 = tables.Float32Col(shape=IMG_SHAPE)
        img16 = tables.Float32Col(shape=IMG_SHAPE)
        img17 = tables.Float32Col(shape=IMG_SHAPE)
        img18 = tables.Float32Col(shape=IMG_SHAPE)
        img19 = tables.Float32Col(shape=IMG_SHAPE)
        lat  = tables.Float32Col()
        lng  = tables.Float32Col()
        img_id = tables.Int64Col()
        urban_share = tables.Float32Col()
elif mode == "mw":
    YEARS = ["0", "10", "15"]
    class IMGData(tables.IsDescription):
        img0 = tables.Float32Col(shape=IMG_SHAPE)
        img10 = tables.Float32Col(shape=IMG_SHAPE)
        img15 = tables.Float32Col(shape=IMG_SHAPE)
        lat  = tables.Float32Col()
        lng  = tables.Float32Col()
        img_id = tables.Int64Col()
        urban_share = tables.Float32Col()

def main():
    # 一時フォルダと出力フォルダの作成
    tempdir = f"{ROOT}/temp_{mode}"
    if not os.path.exists(tempdir):
        os.mkdir(tempdir)
    if not os.path.exists(f"{ROOT}/outputs"):
        os.mkdir(f"{ROOT}/outputs")

    # HDF5ファイルの作成または追記モードで開く
    h5_open_mode = "w" if not os.path.exists(path) else "a"
    h5_file = tables.open_file(path, mode=h5_open_mode)
    if "/data" not in h5_file:
        h5_file.create_table("/", "data", IMGData)
    table = h5_file.get_node("/data")
    
    # すでに処理済みのファイルを記録するテキストファイル
    processed_paths_file = f"{ROOT}/outputs/processed_paths_{mode}.txt"
    if not os.path.exists(processed_paths_file):
        with open(processed_paths_file, "w") as fh:
            pass

    # Google Drive から tfrecord ファイル一覧を取得し、フィルタ・ソート
    GD = GDFolderDownloader(
        root_dir_id, 
        tempdir, os.getcwd() + "/client_secrets.json",
        processed_paths_file)
    GD.file_list = filter(lambda x: ".tfrecord" in x["title"], GD.file_list)

    key = lambda x: int(x["title"].split("-")[-1].replace(".tfrecord",""))
    GD.file_list = sorted(GD.file_list, key=key)

    # 各画像ファイルをダウンロードして処理
    ix = 0
    invalid_data = 0
    outfh_path = f"{ROOT}/outputs/valid_imgs_{mode}.txt"
    out_fh = open(outfh_path, "w" if not os.path.exists(outfh_path) else "a")

    LOG.info("Total Images to download: {}".format(len(GD.file_list)))

    total_imgs = 0
    for fpath in GD.file_iterator():
        img_num = 0
        if fpath is None:
            LOG.info("File exists - Delete it to download again...")
            continue

        it = tfr_data_pipeline(fpath, IMG_ROWS_RAW, IMG_COLS_RAW)

        with tf.compat.v1.Session() as sess:
            while True:
                try:
                    imgs, lat, lng, urban = sess.run(it)
                    img_num += 1

                    urban[np.isnan(urban)] = 0
                    if np.mean(urban) < 0.1:
                        continue  # 都市化割合が低い画像はスキップ

                    lat = lat[IMG_ROWS_RAW//2, IMG_COLS_RAW//2]
                    lng = lng[IMG_ROWS_RAW//2, IMG_COLS_RAW//2]
                    ix += 1
                    out_fh.write("{},{},{},{},{},{}\n".format(
                        fpath, img_num, ix, lat, lng, np.nanmean(urban)))

                    for y in YEARS:
                        table.row["img{}".format(y)] = imgs[y]
                    table.row["urban_share"] = np.nanmean(urban)
                    table.row["lat"] = lat
                    table.row["lng"] = lng
                    table.row["img_id"] = ix

                    table.row.append()
                    total_imgs += 1

                except tf.errors.OutOfRangeError:
                    break
                except tf.errors.DataLossError:
                    invalid_data += 1
                    break

        LOG.info("Wrote: {} images".format(total_imgs))
        with open(processed_paths_file, "a") as fh:
            fh.write(fpath + "\n")
        os.unlink(fpath)  # 一時ファイル削除

    LOG.info("Total images processed: {}".format(ix))
    LOG.info("Invalid data errors: {}".format(invalid_data))
    h5_file.close()


def tfr_data_pipeline(path, img_rows, img_cols):
    # TFRecordのデータ構造を定義してパースするパイプライン
    channel_names = ["{}_{}".format(x,y) for x in CHANNEL_NAMES for y in YEARS]
    other_vars = ["urban", "longitude", "latitude"]
    varnames = channel_names + other_vars

    features = [tf.compat.v1.FixedLenFeature([img_rows*img_cols], tf.float32)] * len(varnames)
    features_dict = dict(zip(varnames, features))

    def parse_example(example_proto):
        parsed_features = tf.compat.v1.parse_single_example(example_proto, features_dict)
        f = lambda x: tf.reshape(x, (img_rows, img_cols))
        imgs = {}
        axes = [2, 1, 0]  # 軸を (channel, row, col) -> (row, col, channel) に並べ替え

        for y in YEARS:
            cname = ["{}_{}".format(x,y) for x in CHANNEL_NAMES]
            ii = tf.stack([f(parsed_features[x]) for x in cname], 0)
            ii = tf.transpose(ii, axes)
            imgs[y] = ii

        urban = f(parsed_features["urban"])
        lat = f(parsed_features["latitude"])
        lng = f(parsed_features["longitude"])

        return imgs, lat, lng, urban

    ds = tf.data.TFRecordDataset(path)
    parsed_ds = ds.map(parse_example)
    it = tf.compat.v1.data.make_one_shot_iterator(parsed_ds)
    return it.get_next()



if __name__=="__main__":
    main()
