import pandas as pd

def get_scale(data: pd.DataFrame, col: str) -> int:
    mx_0 = data[data[col].notna()][col].max()
    mn_0 = data[data[col].notna()][col].min()
    mx = max(mx_0, -mn_0)

    if mx < 1_000:
        return 0
    else:
        return mx / mn

