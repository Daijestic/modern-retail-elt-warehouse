# Trade-offs

## Raw ingestion idempotency strategy

Current strategy:

```text
TRUNCATE raw table
→ reload CSV
→ record ingestion run