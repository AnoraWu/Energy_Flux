import pandas as pd

# 这段代码吧 long form 变成 wide form
df = pd.read_csv('/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/intermediate/combined_ssf_droplets.csv')

df = df.groupby(['Lower_and_Upper_Cloud_Layers','adcode','date']).mean()
df = df.reset_index()

df_wide = df.pivot(
    index=["adcode", "date"], 
    columns="Lower_and_Upper_Cloud_Layers"
)

df_wide.columns = [f"{col}_{layer}" for col, layer in df_wide.columns]

df_wide = df_wide.reset_index()

df_wide.to_csv('/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/intermediate/combined_ssf_droplets_long.csv')