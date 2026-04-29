def validate_dataframe(df, required_columns):
    missing = set(required_columns) - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {missing}")