# Metrics ideas

## Processed transactions (implemented)
- `neo4j_driver_transactions_seconds_count` - counter tracking total number of processed transactions
- `neo4j_driver_transactions_seconds_sum` - counter tracking total execution time
- `neo4j_driver_transactions_seconds_max` - gauge tracking maximum execution time

Tags:
- `database` - target database or `<default database>`
- `state` - `COMMITTED` or `ROLLED_BACK`

This currently covers explicit transactions only. We might want to include autocommit transactions in this metric.
TBC when to finish timing if prefetch logic is enabled.

## Open transactions
- `neo4j_driver_open_transactions` - gauge tracking total number of open transactions

Tags:
- `database` - target database or `<default database>`
- `state` - `ACTIVE` or `TERMINATED`

In general, the `in_use` connections metric is very similar to this.

# Queries
- `neo4j_driver_queries_seconds_count` - counter tracking total number of processed queries
- `neo4j_driver_queries_seconds_sum` - counter tracking total execution time
- `neo4j_driver_queries_seconds_max` - gauge tracking maximum execution time

Tags:
- `database` - target database or `<default database>`
- `tx_type` - `EXPLICIT` or `IMPLICIT`
- `state` - `SUCCEEDED` or `FAILED`

TBC when to finish timing if prefetch logic is enabled.

# Routing requests
- `neo4j_driver_routing_requests_seconds_count` - counter tracking total number of routing requests
- `neo4j_driver_routing_requests_seconds_sum` - counter tracking total execution time
- `neo4j_driver_routing_requests_seconds_max` - gauge tracking maximum execution time

Tags:
- `database` - target database or `<default database>`
