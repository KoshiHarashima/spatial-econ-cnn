"""
このスクリプトを実行するには、以下の手順に従ってください：

1. Google Earth Engine のアカウントを作成
2. GEE の Python API をセットアップ（condaなどで "ee" 環境を作成）
   conda activate ee
3. コマンドラインからこのファイルを実行
   python ".../export_mw_highres_imagery.py"
"""

# Earth Engine API を初期化
import ee
ee.Initialize()

# Surface Reflectance (SR) データ用の雲マスク関数（Landsat 4/5/7）
def cloudMaskL457(image): 
  qa = image.select('pixel_qa')
  cloud = qa.bitwiseAnd(1 << 5).And(qa.bitwiseAnd(1 << 7)).Or(qa.bitwiseAnd(1 << 3))
  mask2 = image.mask().reduce(ee.Reducer.min())
  return(image.updateMask(cloud.Not()).updateMask(mask2))

# TOA（Top of Atmosphere）データ用のシンプルな雲マスク（スコア20以下を採用）
def cloudmask(image): 
  clear = ee.Algorithms.Landsat.simpleCloudScore(image).select(['cloud']).lte(20)
  return(image.updateMask(clear))

# 対象地域（Midwest）の地理領域（長方形）を定義
mw = ee.Geometry.Rectangle(-91.37635548114778, 29.325054203049273, -72.34803516864778, 42.31009875176506)

# 年2000：TOA画像から RGB + パンクロマティック（B8）を取得して合成
toa0 = ee.ImageCollection("LANDSAT/LE07/C01/T1_TOA").map(cloudmask).filterDate(ee.DateRange('2000-05-01', '2000-08-30')).median().select(['B3','B2','B1','B8'],['red_0','green_0','blue_0','B8_0'])
# RGB → HSV 変換 → パンクロでシャープ化 → HSV → RGB に戻す（パンシャープン）
hsv0 = toa0.select(['red_0', 'green_0', 'blue_0']).rgbToHsv()
sharpened0 = ee.Image.cat([hsv0.select('hue'), hsv0.select('saturation'), toa0.select('B8_0')]).hsvToRgb().select(['red','green','blue'],['psred_0','psgreen_0','psblue_0'])

# 年2010
toa10 = ee.ImageCollection("LANDSAT/LE07/C01/T1_TOA").map(cloudmask).filterDate(ee.DateRange('2010-05-01', '2010-08-30')).median().select(['B3','B2','B1','B8'],['red_10','green_10','blue_10','B8_10'])
hsv10 = toa10.select(['red_10', 'green_10', 'blue_10']).rgbToHsv()
sharpened10 = ee.Image.cat([hsv10.select('hue'), hsv10.select('saturation'), toa10.select('B8_10')]).hsvToRgb().select(['red','green','blue'],['psred_10','psgreen_10','psblue_10'])

# 年2015
toa15 = ee.ImageCollection("LANDSAT/LE07/C01/T1_TOA").map(cloudmask).filterDate(ee.DateRange('2015-05-01', '2015-08-30')).median().select(['B3','B2','B1','B8'],['red_15','green_15','blue_15','B8_15'])
hsv15 = toa15.select(['red_15', 'green_15', 'blue_15']).rgbToHsv()
sharpened15 = ee.Image.cat([hsv15.select('hue'), hsv15.select('saturation'), toa15.select('B8_15')]).hsvToRgb().select(['red','green','blue'],['psred_15','psgreen_15','psblue_15'])

# 各年の Surface Reflectance データも追加取得（雲除去後の合成）
ls0 = ee.ImageCollection("LANDSAT/LE07/C01/T1_SR").filterDate(ee.DateRange('2000-05-01', '2000-08-30')).map(cloudMaskL457).median().select(['B3','B2','B1','B4','B5','B6','B7'],['red_0','green_0','blue_0','B4_0','B5_0','B6_0','B7_0'])
ls10 = ee.ImageCollection("LANDSAT/LE07/C01/T1_SR").filterDate(ee.DateRange('2010-05-01', '2010-08-30')).map(cloudMaskL457).median().select(['B3','B2','B1','B4','B5','B6','B7'],['red_10','green_10','blue_10','B4_10','B5_10','B6_10','B7_10'])
ls15 = ee.ImageCollection("LANDSAT/LE07/C01/T1_SR").filterDate(ee.DateRange('2015-05-01', '2015-08-30')).map(cloudMaskL457).median().select(['B3','B2','B1','B4','B5','B6','B7'],['red_15','green_15','blue_15','B4_15','B5_15','B6_15','B7_15']) 

# 人口密度に基づいた都市判定レイヤー（1マイルバッファ）
urban = ee.Image("users/armanucsd/national_bg_urban_buffer1mile").select(['first'], ['urban'])

# Surface Reflectance + 緯度経度 + 都市バッファ + パンシャープンRGB画像をすべて結合
allbands =  ls0.addBands(ls10).addBands(ls15).addBands(ee.Image.pixelLonLat()).addBands(urban).addBands(sharpened0).addBands(sharpened10).addBands(sharpened15)

# 全国の都市ポリゴンの中から、中西部領域にあるものだけを抽出
blobs = ee.FeatureCollection("users/armanucsd/popdbuff_splitblobs_national")
mwblobs = blobs.filterBounds(mw)

# 指定されたfnumの都市領域（blob）に対してTFRecord形式で画像をエクスポート
def outfeat(fnum):
  fnumstr=str(fnum)
  descrip = 'blobs_papi_highres' + fnumstr
  geo = ee.Feature(mwblobs.filterMetadata('fnum', 'equals', fnumstr).first()).geometry()
  
  tfr_opts = {}
  tfr_opts['patchDimensions'] = [96,96]
  tfr_opts['kernelSize'] = [12,12]

  
  task=ee.batch.Export.image.toDrive(
    folder='TFR_p96k12_mwblobs_papi_highres',
    image=allbands.float(),  
    description=descrip,
    scale=15,
    region=geo,
    maxPixels=70e6, 
    fileFormat='TFRecord',
    formatOptions=tfr_opts
  )

  task.start()


# 都市ポリゴンの一部（例: 0〜999）に対してエクスポートを順次実行
# フルセットでは 0～4786 の範囲を分割して実行

##3000,4786
##1000,3000 
##0,1000

for i in range(0,1000):
  outfeat(i)


