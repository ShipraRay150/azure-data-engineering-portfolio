# Test plan

Initial manual checks:

1. Confirm all three Bronze Delta datasets are created.
2. Confirm Silver orders contain no null order_id values.
3. Confirm invalid quantity and price records appear in rejected_orders.
4. Confirm duplicate order IDs are reduced to one record.
5. Reconcile Gold daily sales against the accepted Silver order total.
6. Run the pipeline twice and inspect whether the overwrite behavior is understood.

Next iteration: add automated pytest tests and incremental MERGE tests.
