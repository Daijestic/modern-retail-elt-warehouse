# Ghi chú phát hành

## v1.0.0

Bản phát hành này nâng cấp repository từ một portfolio prototype thành một bản demo kho dữ liệu ELT chạy trong môi trường local, có thể tái lập và lấy cảm hứng từ production.

Các điểm nổi bật:

- môi trường PostgreSQL chạy bằng Docker có thể tái lập
- ingestion bằng Python theo cấu hình, có exit code rõ ràng
- làm mới toàn bộ theo cách atomic thông qua thay thế dữ liệu từ bảng staging
- metadata cho pipeline run, table load và bản ghi bị loại
- tầng landing đã được kiểm soát với các cột provenance
- các model dbt ở tầng staging, core và analytics với hệ test tốt hơn
- độ bao phủ của kiểm thử unit và integration
- workflow CI bằng GitHub Actions
- demo một lệnh qua `python scripts/project_cli.py demo`

Các bước gợi ý để phát hành:

```bash
git tag v1.0.0
git push origin v1.0.0
```

Sau đó tạo GitHub release từ tag `v1.0.0` và dùng phần “Các điểm nổi bật” ở trên làm nội dung mô tả release.
