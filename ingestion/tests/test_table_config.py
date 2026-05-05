from ingestion.table_config import TABLE_CONFIG


def test_table_config_is_not_empty():
    assert len(TABLE_CONFIG) > 0


def test_each_table_config_has_required_keys():
    required_keys = {"name", "file", "table", "pk", "required_columns"}

    for table_cfg in TABLE_CONFIG:
        assert required_keys.issubset(table_cfg.keys())


def test_each_table_config_has_primary_key_in_required_columns():
    for table_cfg in TABLE_CONFIG:
        assert table_cfg["pk"] in table_cfg["required_columns"]