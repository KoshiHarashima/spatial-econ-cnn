import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
import pandas as pd
from models import *              # モデル定義（make_level_model など）
from data_loader import *        # データセット読み込み関数（read_files, get_img_size など）
from tqdm import tqdm            # プログレスバー表示

# GPUメモリの自動調整
tf.random.set_seed(1234567)
physical_devices = tf.config.experimental.list_physical_devices('GPU')
if len(physical_devices) > 0:
    tf.config.experimental.set_memory_growth(physical_devices[0], True)

# 各種ハイパーパラメータ・設定値はコマンドライン引数から取得（sys.argv）
construct = sys.argv[1]  # BG or block
region = sys.argv[2]  # ['national', 'mw']
model_type = sys.argv[3]  # ['base', 'RGB', 'nl']
size = sys.argv[4]  # ['large', 'small']
datatype = sys.argv[5]  # ['inc', 'pop']
resolution = sys.argv[6] # ['high', 'low']
with_feature = get_bool(sys.argv[7])  # [True, False]
epochs = int(sys.argv[8])
data_dir = sys.argv[9]  # dir of dataset
out_dir = sys.argv[10]  # dir of output
weight_dir = sys.argv[11]
lr = float(sys.argv[12])
l2 = float(sys.argv[13])
bs = int(sys.argv[14])
ds = int(sys.argv[15])
nf = int(sys.argv[16])
dr = float(sys.argv[17])
all_sample = get_bool(sys.argv[18]) # [True, False]

# 入力データに含まれる年度（予測する年）
if datatype == "inc":
    years = ['0', '10', '15']
else:
    years = ['0', '10']

# 重みファイルのパス（ハイパーパラメータに応じて動的に構築）
weight_dir = '{}/{}_{}_{}_level_{}{}{}_{}_{}{}/checkpoints/{}_{}_{}_{}_{}_{}'\
    .format(weight_dir, construct, size, region, model_type, '_feature' if with_feature else '',
            '_high' if resolution == 'high' else '', datatype, epochs, '_all' if all_sample else '', lr, l2,
            bs, ds, nf, dr)
# TFRecordファイルのパス（train/valid/testごとに使い分け）
ds_dir = '{}/{}_{}_{}_{}/{}_{}_{}_{}_{}_*-of-*.tfrecords'\
    .format(data_dir, size, construct, '{}', region, '{}', construct, size, '{}', region)

def main():
    df = pd.DataFrame()
    img_size, _, n_origin_bands, n_bands, res = get_img_size(size, model_type, region, resolution)
    if region == "mw":
        feature_description = get_feature_description(feature_type='mw_15')
    else:
        feature_description = get_feature_description(feature_type='test')
    # TFRecordデータ読み込み（train, validation, test）
    train = read_files(ds_dir.format(15, 'train', 15), lambda x: parse(x, feature_description, img_size, n_origin_bands, n_bands, res))
    valid = read_files(ds_dir.format(15, 'validation', 15), lambda x: parse(x, feature_description, img_size, n_origin_bands, n_bands, res))
    test = read_files(ds_dir.format(15, 'test', 15), lambda x: parse(x, feature_description, img_size, n_origin_bands, n_bands, res))
    
    # モデルの構築・重みの読み込み
    model = make_level_model(img_size, n_bands, l2, nf, dr, with_feature)
    model.compile(optimizer=tf.keras.optimizers.Adam(lr), loss="mean_squared_error", metrics=[RSquare()])
    model.load_weights(weight_dir).expect_partial()
    
    # 各セットに対して予測実施
    df = predict(train, model, df)
    df = predict(valid, model, df)
    df = predict(test, model, df)

    # 結果をCSVファイルに出力
    df.to_csv('{}/{}_{}_{}_level_{}{}{}_{}_predictions{}.csv'.format(out_dir, construct, size, region, model_type,
                                                                     '_feature' if with_feature else '',
                                                                     '_high' if resolution == 'high' else '',
                                                                     datatype, '_all' if all_sample else ''),
              index=False)
    print('complete!')


def predict(ds, model, df):
    for img, features, img_id in tqdm(ds.as_numpy_iterator()):
        if with_feature:
            # 特徴量の有無に応じて入力形式を変更
            predictions = model((img, features), training=False)
        else:
            predictions = model(img, training=False)
        # 年度ごとの予測値を記録
        row = {'img_id': img_id}
        row.update({'{}'.format(year): predictions[idx].numpy()[0] for idx,year in enumerate(years)})
        df = df.append(row, ignore_index=True)
    return df

def parse(serialized_example, feature_description, img_size, n_origin_bands, n_bands, res):
    example = tf.io.parse_single_example(serialized_example, feature_description)
     # 複数年の画像をスタック（例：2000, 2010, 2015）
    image = tf.stack([tf.clip_by_value(tf.reshape(tf.io.parse_tensor(example['image'+y if res == '' else paste_string(['image', res, y])], out_type=float),(img_size, img_size, n_origin_bands))[:, :, 0:n_bands], 0, 1) for y in years], 0)
    
    # 追加の統計特徴量（34次元ベクトル）を年数分複製
    features = tf.io.parse_tensor(example['baseline_features'], out_type=float)
    features = tf.reshape(features, (34,))
    features = tf.stack([features for y in years], 0)
    img_id = example['img_id']

    return image, features, img_id


if __name__ == "__main__":
    main()
