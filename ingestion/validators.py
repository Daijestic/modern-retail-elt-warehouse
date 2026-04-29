REQUIRED_COLUMNS = [
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
]

def validate_dataframe(df):
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")