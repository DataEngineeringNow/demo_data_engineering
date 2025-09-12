def run_dq_checks(df, required_columns=None):
    if required_columns:
        for col in required_columns:
            if df[col].isnull().any():
                raise ValueError(f"Nulls found in required column: {col}")
    if df.empty:
        raise ValueError("DataFrame is empty after transformation!")
    print("Data quality checks passed.")
