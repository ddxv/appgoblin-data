import pandas as pd
from dbcon.queries import query_store_apps_companies, query_store_apps

df = query_store_apps_companies()
df.to_csv("store_apps_companies.tsv", sep="\t", index=False)
