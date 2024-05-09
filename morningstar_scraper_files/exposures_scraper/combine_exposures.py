import pandas as pd
import numpy as np
import os
import os.path

sheet_names = [exp for exp in os.listdir() if exp.endswith(".xlsx")]

filename = "combined_exposures.xlsx"
for sheet_name in sheet_names:
    exp = pd.read_excel(sheet_name, index_col = 0)
    sheet_name = sheet_name.split(".")[0]
    if os.path.exists(filename):
        with pd.ExcelWriter(filename, engine="openpyxl", mode="a") as wf:
            exp.to_excel(wf,sheet_name)
    else:
        with pd.ExcelWriter(filename, engine="openpyxl", mode="w") as wf:
            exp.to_excel(wf, sheet_name)
