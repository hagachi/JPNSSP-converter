import pandas as pd
from pyprojroot import here

# Path settings ---------------------------------------
root_dir = here()
data_dir = root_dir / 'data'
output_dir = root_dir / 'output'
pq_dir = output_dir / 'pop-mesh-parquet'
csv_dir = output_dir / 'pop-mesh-csv'
if not pq_dir.exists():
    pq_dir.mkdir()
    csv_dir.mkdir()

# Variable setings ------------------------------------
scenarios = ['SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5']
years = ['2015', '2020', '2025', '2030', '2035', '2040', '2045', '2050', '2055', '2060', '2065', '2070', '2075', '2080', '2085', '2090', '2095', '2100']


for year in years:
    print(f'{year}')
    for scenario in scenarios:
        print(f'    {scenario} processing...')
        merged_df = pd.read_parquet(output_dir / f'{scenario}_{year}.parquet')
        # rename the total population and meshcode column
        merged_df = merged_df.rename(columns={merged_df.columns[-1]: 'total',
                                              merged_df.columns[0]: 'meshcode'})
        # save only total population > 0
        merged_df = merged_df.query('total > 0')
        print('        Saving parquet...')
        merged_df.to_parquet(pq_dir / f'{scenario}_{year}.parquet')
        print('        Saving CSV...')
        merged_df.to_csv(csv_dir / f'{scenario}_{year}.csv', index=False)
