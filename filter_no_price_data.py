import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description='Remove rows with no_price_data in Strike_Price')
    parser.add_argument('--csv', required=True, help='Input CSV path')
    parser.add_argument('--out', required=True, help='Output CSV path')
    args = parser.parse_args()

    df = pd.read_csv(args.csv, low_memory=False)
    if 'Strike_Price' not in df.columns:
        raise SystemExit("Missing column: Strike_Price")

    mask = df['Strike_Price'].astype(str) != 'no_price_data'
    kept = df[mask]
    kept.to_csv(args.out, index=False)
    print(f"Input rows: {len(df)}")
    print(f"Output rows: {len(kept)}")
    print(f"Removed rows: {len(df) - len(kept)}")


if __name__ == '__main__':
    main()
