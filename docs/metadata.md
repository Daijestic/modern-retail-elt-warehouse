# Metadata và khả năng quan sát

## Các bảng metadata

### `metadata.pipeline_runs`

Một bản ghi cho mỗi lần chạy ingestion end-to-end.

Các trường chính:

- `run_id`
- `status`
- `started_at`
- `finished_at`
- `duration_seconds`
- `tables_succeeded`
- `tables_partial`
- `tables_failed`

### `metadata.ingestion_runs`

Một bản ghi cho mỗi lần nạp bảng trong một pipeline run.
Đây là nơi lưu trạng thái tổng quát của lần nạp bảng, bao gồm cả lỗi cấp file.

Các trường chính:

- `table_run_id`
- `run_id`
- `source_name`
- `source_file`
- `target_schema`
- `target_table`
- `file_size_bytes`
- `file_checksum`
- `schema_version`
- `load_strategy`
- `status`
- `rows_read`
- `rows_loaded`
- `rows_rejected`
- `duplicate_count`
- `started_at`
- `finished_at`
- `duration_seconds`
- `error_type`
- `error_message`

Ghi chú:

- khi lỗi xảy ra ở cấp file hoặc ở cấp lần nạp bảng, `error_type` và `error_message` phản ánh lỗi đó
- khi chỉ có lỗi cấp dòng, bảng thường ở trạng thái `PARTIAL_SUCCESS`, `rows_rejected > 0`, còn `error_type` và `error_message` để trống
- nếu mọi dòng đều bị loại, bảng sẽ `FAILED` với `error_type = NO_VALID_ROWS`

### `metadata.ingestion_rejections`

Một bản ghi cho mỗi dòng nguồn bị loại.
Đây chỉ là nơi lưu lỗi cấp dòng, không phải nơi lưu lỗi data contract của cả file.

Các trường chính:

- `rejection_id`
- `run_id`
- `table_run_id`
- `source_file`
- `target_table`
- `source_row_number`
- `reason_code`
- `reason_detail`
- `raw_record`
- `rejected_at`

## Các truy vấn hữu ích

### Lần chạy pipeline thành công gần nhất

```sql
select *
from metadata.pipeline_runs
where status = 'SUCCESS'
order by finished_at desc
limit 1;
```

### Các lần nạp bảng bị lỗi

```sql
select
    run_id,
    source_name,
    source_file,
    target_table,
    status,
    error_type,
    error_message,
    finished_at
from metadata.ingestion_runs
where status = 'FAILED'
order by finished_at desc;
```

### Số lượng bản ghi bị loại theo reason code

```sql
select
    reason_code,
    count(*) as rejected_rows
from metadata.ingestion_rejections
group by reason_code
order by rejected_rows desc, reason_code;
```

### Số dòng đã nạp theo từng nguồn dữ liệu

```sql
select
    source_name,
    sum(rows_loaded) as rows_loaded
from metadata.ingestion_runs
where status in ('SUCCESS', 'PARTIAL_SUCCESS')
group by source_name
order by source_name;
```

### Thời gian nạp trung bình theo từng nguồn

```sql
select
    source_name,
    round(avg(duration_seconds), 3) as avg_duration_seconds
from metadata.ingestion_runs
group by source_name
order by source_name;
```

### Độ mới dữ liệu theo từng bảng nguồn

```sql
select
    target_table,
    max(finished_at) as latest_finished_at
from metadata.ingestion_runs
where status in ('SUCCESS', 'PARTIAL_SUCCESS')
group by target_table
order by target_table;
```
