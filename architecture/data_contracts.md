# 📊 Data Contracts — Unified Retail Platform

This document defines the schema contracts for all upstream data sources (API, streaming, and batch).
These contracts act as the single source of truth for ingestion, validation, and downstream transformations.

---

# 🔹 API Sources

## 1️⃣ E-commerce Orders API (`/orders`)

| Field Name       | Data Type          | Required | Example Value                          |
| ---------------- | ------------------ | -------- | -------------------------------------- |
| order_id         | string (UUID)      | ✅ Yes    | "a3f1c9e2-7d4b-4c1a-9f2e-123456789abc" |
| customer_id      | string             | ✅ Yes    | "CUST_10234"                           |
| order_status     | string             | ✅ Yes    | "DELIVERED"                            |
| order_date       | datetime (ISO8601) | ✅ Yes    | "2026-03-25T14:32:10Z"                 |
| channel          | string             | ✅ Yes    | "mobile_app"                           |
| total_amount     | float              | ✅ Yes    | 149.99                                 |
| currency         | string             | ✅ Yes    | "USD"                                  |
| discount_amount  | float              | ❌ No     | 10.00                                  |
| shipping_cost    | float              | ❌ No     | 5.99                                   |
| payment_method   | string             | ✅ Yes    | "credit_card"                          |
| shipping_address | string             | ❌ No     | "Cairo, Egypt"                         |

---

## 2️⃣ Marketing Campaigns API (`/campaigns`)

| Field Name      | Data Type | Required | Example Value          |
| --------------- | --------- | -------- | ---------------------- |
| campaign_id     | string    | ✅ Yes    | "CMP_7890"             |
| campaign_name   | string    | ✅ Yes    | "Ramadan Sale 2026"    |
| channel         | string    | ✅ Yes    | "email"                |
| start_date      | datetime  | ✅ Yes    | "2026-03-01T00:00:00Z" |
| end_date        | datetime  | ❌ No     | "2026-03-31T23:59:59Z" |
| budget          | float     | ❌ No     | 50000.00               |
| target_audience | string    | ❌ No     | "Returning Customers"  |
| status          | string    | ✅ Yes    | "ACTIVE"               |

---

# 🔹 Streaming Sources

## 3️⃣ Order Events (`/events/orders`)

| Field Name   | Data Type     | Required | Example Value          |
| ------------ | ------------- | -------- | ---------------------- |
| event_id     | string (UUID) | ✅ Yes    | "evt_123abc"           |
| event_type   | string        | ✅ Yes    | "ORDER_SHIPPED"        |
| occurred_at  | datetime      | ✅ Yes    | "2026-03-25T15:10:00Z" |
| order_id     | string        | ✅ Yes    | "ORD_45678"            |
| customer_id  | string        | ✅ Yes    | "CUST_10234"           |
| status       | string        | ✅ Yes    | "SHIPPED"              |
| warehouse_id | string        | ❌ No     | "WH_01"                |
| carrier      | string        | ❌ No     | "DHL"                  |

**Note:** `event_type` represents the action that occurred (e.g., ORDER_SHIPPED), while `status` represents the resulting state after the event (e.g., SHIPPED).

---

## 4️⃣ Clickstream Events (`/events/clickstream`)

| Field Name   | Data Type     | Required | Example Value          |
| ------------ | ------------- | -------- | ---------------------- |
| event_id     | string (UUID) | ✅ Yes    | "clk_987xyz"           |
| event_type   | string        | ✅ Yes    | "PRODUCT_VIEW"         |
| occurred_at  | datetime      | ✅ Yes    | "2026-03-25T16:00:00Z" |
| user_id      | string        | ❌ No     | "USER_5678"            |
| session_id   | string        | ✅ Yes    | "sess_12345"           |
| page_url     | string        | ✅ Yes    | "/product/iphone-15"   |
| product_id   | string        | ❌ No     | "PROD_111"             |
| device_type  | string        | ❌ No     | "mobile"               |
| geo_location | string        | ❌ No     | "Egypt"                |

---

## 5️⃣ Store Footfall Events (`/events/footfall`)

| Field Name    | Data Type     | Required | Example Value          |
| ------------- | ------------- | -------- | ---------------------- |
| event_id      | string (UUID) | ✅ Yes    | "foot_321abc"          |
| event_type    | string        | ✅ Yes    | "STORE_ENTRY"          |
| occurred_at   | datetime      | ✅ Yes    | "2026-03-25T17:20:00Z" |
| store_id      | string        | ✅ Yes    | "STORE_45"             |
| direction     | string        | ✅ Yes    | "IN"                   |
| customer_type | string        | ❌ No     | "new"                  |
| sensor_id     | string        | ❌ No     | "SENSOR_9"             |

---

# 🔹 Batch Sources (CSV)

## 6️⃣ Sales Transactions (CSV)

| Field Name       | Data Type | Required | Example Value          |
| ---------------- | --------- | -------- | ---------------------- |
| transaction_id   | string    | ✅ Yes    | "TXN_10001"            |
| order_id         | string    | ✅ Yes    | "ORD_45678"            |
| customer_id      | string    | ❌ No     | "CUST_10234"           |
| product_id       | string    | ✅ Yes    | "PROD_111"             |
| quantity         | integer   | ✅ Yes    | 2                      |
| unit_price       | float     | ✅ Yes    | 50.00                  |
| total_price      | float     | ✅ Yes    | 100.00                 |
| transaction_date | datetime  | ✅ Yes    | "2026-03-25T14:35:00Z" |
| store_id         | string    | ❌ No     | "STORE_45"             |
| channel          | string    | ✅ Yes    | "online"               |

---

## 7️⃣ Product Catalog (CSV)

| Field Name   | Data Type | Required | Example Value          |
| ------------ | --------- | -------- | ---------------------- |
| product_id   | string    | ✅ Yes    | "PROD_111"             |
| product_name | string    | ✅ Yes    | "iPhone 15"            |
| category     | string    | ✅ Yes    | "Electronics"          |
| subcategory  | string    | ❌ No     | "Smartphones"          |
| brand        | string    | ❌ No     | "Apple"                |
| price        | float     | ✅ Yes    | 999.99                 |
| cost         | float     | ❌ No     | 700.00                 |
| launch_date  | datetime  | ❌ No     | "2025-09-01T00:00:00Z" |
| is_active    | boolean   | ✅ Yes    | true                   |

---

## 8️⃣ Inventory Snapshot (CSV)

| Field Name     | Data Type | Required | Example Value          |
| -------------- | --------- | -------- | ---------------------- |
| snapshot_date  | datetime  | ✅ Yes    | "2026-03-25T00:00:00Z" |
| product_id     | string    | ✅ Yes    | "PROD_111"             |
| store_id       | string    | ✅ Yes    | "STORE_45"             |
| warehouse_id   | string    | ❌ No     | "WH_01"                |
| stock_on_hand  | integer   | ✅ Yes    | 120                    |
| stock_reserved | integer   | ❌ No     | 20                     |
| reorder_level  | integer   | ❌ No     | 50                     |

---

## 9️⃣ Customer Master Data (CSV)

| Field Name        | Data Type | Required | Example Value                               |
| ----------------- | --------- | -------- | ------------------------------------------- |
| customer_id       | string    | ✅ Yes    | "CUST_10234"                                |
| first_name        | string    | ✅ Yes    | "Ahmed"                                     |
| last_name         | string    | ✅ Yes    | "Hassan"                                    |
| email             | string    | ❌ No     | "[ahmed@email.com](mailto:ahmed@email.com)" |
| phone_number      | string    | ❌ No     | "+201234567890"                             |
| gender            | string    | ❌ No     | "Male"                                      |
| birth_date        | date      | ❌ No     | "1995-05-10"                                |
| country           | string    | ❌ No     | "Egypt"                                     |
| registration_date | datetime  | ✅ Yes    | "2024-01-15T10:00:00Z"                      |
| loyalty_tier      | string    | ❌ No     | "Gold"                                      |

---

# 📌 Notes

* All timestamps must follow ISO 8601 format (UTC).
* CSV sources must maintain consistent column ordering.
* All IDs should be unique and consistent across systems.
* Nullable fields simulate real-world incomplete data scenarios.
* Streaming sources are designed for micro-batch ingestion patterns.
* Batch sources simulate daily/hourly ingestion workloads.
