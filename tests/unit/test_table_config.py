from ingestion.table_config import LANDING_METADATA_COLUMNS, TABLE_CONFIG


def test_table_config_is_not_empty():
    assert len(TABLE_CONFIG) >= 2


def test_each_table_config_has_required_attributes():
    for table_config in TABLE_CONFIG:
        assert table_config.name
        assert table_config.file_name.endswith(".csv")
        assert table_config.target_table.startswith("raw_")
        assert len(table_config.primary_key) >= 1
        assert len(table_config.required_columns) >= 1


def test_target_columns_include_landing_metadata():
    for table_config in TABLE_CONFIG:
        for metadata_column in LANDING_METADATA_COLUMNS:
            assert metadata_column in table_config.target_columns
