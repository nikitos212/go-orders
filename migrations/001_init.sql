-- ========== orders ==========
CREATE TABLE IF NOT EXISTS orders (
  order_uid TEXT PRIMARY KEY,
  track_number TEXT,
  entry TEXT,
  locale TEXT,
  internal_signature TEXT,
  customer_id TEXT,
  delivery_service TEXT,
  shardkey TEXT,
  sm_id INT,
  date_created TIMESTAMPTZ,
  oof_shard TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_date_created ON orders(date_created);

-- ========== delivery (one-to-one per order) ==========
CREATE TABLE IF NOT EXISTS delivery (
  order_uid TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
  name TEXT,
  phone TEXT,
  zip TEXT,
  city TEXT,
  address TEXT,
  region TEXT,
  email TEXT
);

-- ========== payment (one-to-one per order) ==========
CREATE TABLE IF NOT EXISTS payment (
  order_uid TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
  transaction_id TEXT,
  request_id TEXT,
  currency TEXT,
  provider TEXT,
  amount BIGINT,
  payment_dt BIGINT, -- unix epoch seconds, как в примере
  bank TEXT,
  delivery_cost BIGINT,
  goods_total BIGINT,
  custom_fee BIGINT
);

-- ========== items (many per order) ==========
CREATE TABLE IF NOT EXISTS items (
  id SERIAL PRIMARY KEY,
  order_uid TEXT REFERENCES orders(order_uid) ON DELETE CASCADE,
  chrt_id BIGINT,
  track_number TEXT,
  price BIGINT,
  rid TEXT,
  name TEXT,
  sale INT,
  size TEXT,
  total_price BIGINT,
  nm_id BIGINT,
  brand TEXT,
  status INT
);

CREATE INDEX IF NOT EXISTS idx_items_order_uid ON items(order_uid);
