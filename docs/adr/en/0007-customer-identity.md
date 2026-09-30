# ADR-0007: Customers are identified by `customer_unique_id`

**Status:** accepted

## Context
In Olist, `customer_id` is created **per order**. The real person is `customer_unique_id`.
Counting `customer_id` counts orders, and every repeat-customer metric comes out as zero.

## Decision
- Silver keeps `customer_id` as delivered, because orders reference it.
- `gold.dim_customer` has one row per `customer_unique_id`. Attributes come from the person's
  most recent order, along with `order_count` and `first_order_ts`.
- The fact table carries `customer_unique_id`, resolved through the customer record of the
  order.

## Consequences
- Test: `test_dim_customer_counts_people_not_customer_ids`.
