import pandas as pd, numpy as np
df = pd.read_excel(r'C:\Users\anton\Desktop\project\Nando_pp\metrics\trafo_loading_compare.xlsx', sheet_name='diff_timeseries_pp')
arr = df.drop(columns=['Unnamed: 0','GLOBAL_MEAN_DIFF_pp'], errors='ignore').to_numpy().flatten()
arr = arr[~np.isnan(arr.astype(float))].astype(float)
print('Overall MAE  (flattened diffs):', round(np.mean(np.abs(arr)), 4))
print('Overall Bias (flattened diffs):', round(np.mean(arr), 4))
print('N data points:', len(arr))

# Also show the global summary row
s = pd.read_excel(r'C:\Users\anton\Desktop\project\Nando_pp\metrics\trafo_loading_compare.xlsx', sheet_name='summary')
glob = s.iloc[0]
print()
print('Summary global row:')
print(f'  Mean of per-trafo MAE:  {glob["MAE_pp"]:.4f}')
print(f'  Mean of per-trafo Bias: {glob["Bias_pp"]:.4f}')
print(f'  Max of per-trafo MaxAbs:{glob["MaxAbs_pp"]:.4f}')
print(f'  Matched trafos: {len(s)-1}')
