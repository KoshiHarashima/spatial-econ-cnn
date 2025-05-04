"""
このスクリプトを実行するには、以下の手順に従ってください：

1. Google Earth Engine のアカウントを作成
2. GEE の Python API をセットアップ（condaなどで "ee" 環境を作成）
   conda activate ee
3. コマンドラインからこのファイルを実行
   python ".../export_mw_highres_imagery.py"
"""
#アメリカの国勢調査ブロック単位で、2000年・2010年の夜間光強度の合計を人口規模別に集計・エクスポートする処理

#アメリカの州別・ブロック別・人口帯別に、DMSP-OLS 夜間光（2000年/2010年）の合計値を集計
#Google Drive に CSV 形式で保存
#都市化の進行や人口密度との関連を後から分析するための前処理データとして使用

import ee
ee.Initialize() # Earth Engine API を初期化

# 2010年の米国国勢調査ブロックのベクターデータ
blocks = ee.FeatureCollection("TIGER/2010/Blocks")

# DMSP-OLS 夜間光データ（2000年と2010年）
dmsp00 = ee.Image('NOAA/DMSP-OLS/NIGHTTIME_LIGHTS/F142000').select('avg_vis')
dmsp10 = ee.Image('NOAA/DMSP-OLS/NIGHTTIME_LIGHTS/F182010').select('avg_vis')

states = ['01', '04', '05', '08', '09', '10', '11', '12', '13', '19', '16', '17', '18', '20', '21', '22', '25', '24', '23', '26', '27', '29', '28', '30', '37', '38', '31', '33', '34', '35', '32', '36', '39', '40', '41', '42', '44', '45', '46', '47', '49', '51', '50', '53', '55', '54', '56'] # 全米州（50州＋DC等）のFIPSコード一覧
# 州ごと＆人口帯ごとに、ブロック単位の夜間光合計値を計算し、CSVでエクスポート
sol = function(state,split){
  
  if (split === undefined || split === null){var split = 0}

      desc00 = ee.String('dmps10rawsum00_blocks_').cat(state).cat(split.toString()).cat('of5')
      descrip00 = desc00.getInfo()

      desc10 = ee.String('dmps10rawsum10_blocks_').cat(state).cat(split.toString()).cat('of5')
      descrip10 = desc10.getInfo()
  # split == 0 のときは州全体のブロック
  if (split==0) filtered_blocks = blocks.filter(ee.Filter.eq('statefp10',state))
  # 各split値に対応する人口帯別のフィルタ（pop10 = 人口）
  if (split==-1) filtered_blocks = blocks.filter(ee.Filter.and(ee.Filter.eq('statefp10',state),ee.Filter.lte('pop10',1)))

  if (split==1) filtered_blocks = blocks.filter(ee.Filter.and(ee.Filter.eq('statefp10',state),ee.Filter.gt('pop10',1),ee.Filter.lte('pop10',3))) 

  if (split==2) filtered_blocks = blocks.filter(ee.Filter.and(ee.Filter.eq('statefp10',state),ee.Filter.gt('pop10',3),ee.Filter.lte('pop10',10))) 
  if (split==3) filtered_blocks = blocks.filter(ee.Filter.and(ee.Filter.eq('statefp10',state),ee.Filter.gt('pop10',10),ee.Filter.lte('pop10',50))) 
  if (split==4) filtered_blocks = blocks.filter(ee.Filter.and(ee.Filter.eq('statefp10',state),ee.Filter.gt('pop10',50),ee.Filter.lte('pop10',100))) 
  if (split==5) filtered_blocks = blocks.filter(ee.Filter.and(ee.Filter.eq('statefp10',state),ee.Filter.gt('pop10',100)))
  
  # 2000年の夜間光合計
  stats00 = dmsp00.reduceRegions({
    reducer: ee.Reducer.sum(),
    collection: filtered_blocks,
    scale: dmsp00.projection().nominalScale(),
    tileScale: 2
  })
  # 2010年の夜間光合計
  stats10 = dmsp10.reduceRegions({
    reducer: ee.Reducer.sum(),
    collection: filtered_blocks,
    scale: dmsp10.projection().nominalScale(),
    tileScale: 2
  })
  # FeatureCollection に変換
  sol_out00 = ee.FeatureCollection(stats00)
  sol_out10 = ee.FeatureCollection(stats10)

  Export.table.toDrive({
    collection: sol_out00,
    description: descrip00,
    folder: 'extract_imagery',
    fileFormat: 'CSV'
    })
  
  Export.table.toDrive({
    collection: sol_out10,
    description: descrip10,
    folder: 'extract_imagery',
    fileFormat: 'CSV'
    })

}


# 2000年に対象とした大きな州
#for 2000
bigstates00 = ['06','23','41','48']

# 2010年に対象とした大きな州
#for 2010
bigstates10 = ['06','12','48']

# 州ごとに人口帯で集計エクスポート（エクスポートは非同期でGEEサーバー側で進行）
sol_split = function(state){
  sol(state,-1);
  sol(state,1);
  sol(state,2);
  sol(state,3);
  sol(state,4);
  sol(state,5);
}  

bigstates.map(sol_split)


