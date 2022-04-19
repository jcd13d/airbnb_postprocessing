import pandas as pd


if __name__ == "__main__":

    df = pd.read_parquet("data/temp")
    for col in df.columns:
        print(col)
        print(df[col])