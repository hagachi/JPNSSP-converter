# NetCDFで公開されているJPNSSP_3meshのデータをCSV形式に変換するツール
- Date: 2024-04-08
- Author: Chihiro Haga (OU, S21-T4-(3))


## データの説明
本データは，「環境研究総合推進費2-1805成果（日本版SSP3次メッシュ人口シナリオ第2版）（https://adaptation-platform.nies.go.jp/socioeconomic/population.html）」の
「移動率設定」（JPNSSP_3mesh）のnetCDF形式のデータをCSV形式に変換したものです．
データ内の各列の説明は以下のとおりです．

- meshcode: 3次メッシュコード
- FXX-YY / MXX-YY: XX歳からYY歳までの女性（F）・男性（M）の人口密度（pop/km2）
- total: メッシュ内の総人口密度（pop/km2）

3次メッシュコードをキーにして，既存の3次メッシュのGISデータにテーブル結合してご利用ください．


## 変換方法

1. ssp-pop-converter.py
    1. netCDFで公開されている元データの各メッシュの緯度経度情報（左下の角の緯度経度）から，メッシュの中心の緯度経度を算出
    2. 各メッシュの中心の緯度経度から，3次メッシュのメッシュIDを推定（https://www.geospatial.jp/ckan/dataset/biodic-mesh/resource/38bd3651-120e-480f-99cf-7bb89cad7a05）
    3. SSPシナリオ・年別に，3次メッシュ別の性別・年齢別の人口データをCSV形式で出力
2. parquet-to-csv-converter.py
    1. 1.でparquet形式に保存したデータをCSV形式で使いやすいように変換する
3. data-validation.ipynb
    1. 予期せぬ誤りがないかを検証するためのコード


## 備考
各SSPシナリオ・各年において，total（総人口密度）がゼロ以上のメッシュのみを抽出しています．
