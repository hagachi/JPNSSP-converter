import netCDF4 as nc
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyprojroot import here
from tqdm import tqdm
import folium
import numpy as np

# Path settings ---------------------------------------
root_dir = here()
data_dir = root_dir / 'data'
output_dir = root_dir / 'output'
if not output_dir.exists():
    output_dir.mkdir()
    (output_dir / 'population-geoparquet').mkdir()
    (output_dir / 'population-parquet').mkdir()


# Variable setings ------------------------------------
scenarios = ['SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5']
# scenario = 'SSP1'
years = ['2015', '2020', '2025', '2030', '2035', '2040', '2045', '2050', '2055', '2060', '2065', '2070', '2075', '2080', '2085', '2090', '2095', '2100']
# year = '2100'


# 3次メッシュデータを読み込む ------------------------------
mesh_name = data_dir / 'mesh1' / 'mesh1.parquet'
if not mesh_name.exists():
    gdf_mesh = gpd.read_file(data_dir / 'mesh1' / 'mesh1.shp')[['Name', 'geometry']] # Name: メッシュコード, geometry: メッシュの形状
    gdf_mesh.to_parquet(mesh_name)


gdf_mesh = gpd.read_parquet(mesh_name)


for year in years:
    # netCDFファイルを読み込む ---------------------------------
    for scenario in scenarios:
        print(f'{year} - {scenario} processing...')
        ncdf_names = list(data_dir.glob(f'ssp_3mesh/{scenario}/{scenario}_{year}/LocalSSP_*.nc4'))
        for i, ncdf_name in enumerate(ncdf_names):
            print(f'Processing {ncdf_name.stem}')
            print(f'  {i} / {len(ncdf_names)}')
            joined_name = output_dir / 'population-geoparquet' / f'{ncdf_name.stem}.parquet'
            if joined_name.exists():
                print('  Already exists')
                continue
            else:
                identifier = ncdf_name.stem.split('_')
                # netCDFファイルを読み込む
                dataset = nc.Dataset(ncdf_name)
                # 人口密度の変数
                # import pdb; pdb.set_trace()
                vname = [v for v in dataset.variables.keys() if identifier[-3] in v or identifier[-2] in v][0]
                variable = dataset.variables[vname]
                # variable = dataset.variables[f'{identifier[-3]}_{identifier[-2]}']
                # 日本の陸域のマスク
                jpn_mask = ~variable[:, :].mask
                # 緯度経度の配列を取得
                lat = dataset.variables['lat'][:]
                lon = dataset.variables['lon'][:]
                # 緯度経度の解像度を取得
                lat_res = abs(lat[2] - lat[1])
                lon_res = abs(lon[2] - lon[1])
                # 人口netCDFデータをポイントデータに変換する --------------------------------------
                # 陸域のマスク上の人口データを取得する 15秒ほどかかる
                points = []
                variables = []
                for i in tqdm(range(len(lon))):
                    for j in range(len(lat)):
                        if jpn_mask[j, i]:
                            # メッシュの左下座標から中心座標に変換する
                            points.append(Point(lon[i] + lon_res / 2, lat[j] + lat_res / 2))
                            variables.append(variable[j, i])
                # 人口ポイントのgeodataframeを作成
                gdf_points = gpd.GeoDataFrame({
                    'geometry': points,
                    'value': variables
                }, crs=gdf_mesh.crs)
                gdf_points = gdf_points.astype({'value': 'float64'}) # 変数をfloat64に変換
                # 人口のポイントデータを3次メッシュデータに空間結合する -------------------------------
                joined_gdf = gpd.sjoin(gdf_mesh, gdf_points, how='left', op='intersects')
                # !!!!!!!! メッシュの数はポイントの数と一致しているか !!!!!!!!!!!
                assert gdf_points.shape[0] == joined_gdf.query('value >= 0').shape[0]
                assert joined_gdf.shape[0] == len(set(joined_gdf['Name'].values))
                joined_gdf = joined_gdf.drop(columns=['index_right'])
                # 列名を変更する (ファイル名を_で分割し、末尾から3つめが齢級)
                joined_gdf.rename({'Name': 'mesh3', 'value': identifier[-3]}, inplace=True, axis=1)
                # ファイルを保存する
                joined_gdf.to_parquet(joined_name)
                # 空間情報を削除したデータを保存する
                joined_gdf.drop(columns=['geometry']).to_parquet(output_dir / 'population-parquet' / f'{ncdf_name.stem}.parquet')
                # netCDFファイルを閉じる
                dataset.close()

        # シナリオ別・年別に一つのファイルに集計する -------------------------------------
        fnames = list((output_dir / 'population-parquet').glob(f'*{scenario}_Population_{year}_*.parquet'))
        fnames.sort()
        dfs = [pd.read_parquet(fname) for fname in fnames]
        for i, df in enumerate(tqdm(dfs)):
            if i == 0:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on='mesh3', how='outer')
                if 'index_right' in merged_df.columns:
                    merged_df.drop(columns='index_right', inplace=True)

        # データ量削減のために、人口0のメッシュのデータを削除する
        # TODO: ここは要検討。もともと居住していないのか、0になったのか。
        # merged_df = merged_df.query(f'{year} >= 0')
        # merged_df.to_csv(output_dir / f'{scenario}_{year}.csv', index=False)
        merged_df.to_parquet(output_dir / f'{scenario}_{year}.parquet', index=False)

        # データの整合性のチェック
        # merged_df['2100'].sum() / merged_df.drop(columns=['mesh3', '2100']).sum().sum()

        # # メッシュデータに結合して保存する
        # merged_gdf = gdf_mesh.merge(merged_df, left_on  = 'Name', right_on = 'mesh3')
        # merged_gdf.drop(columns=['Name'], inplace=True)
        # merged_gdf.to_file(output_dir / f'{scenario}_{year}.shp')
