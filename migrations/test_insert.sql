INSERT INTO orders (order_uid, track_number, entry, locale, customer_id, date_created)
VALUES ('b563feb7b2b84b6test1','WBILMTESTTRACK','WBILL','en','test','2021-11-26T06:22:19Z');

INSERT INTO delivery (order_uid, name, phone, zip, city, address, region, email)
VALUES ('b563feb7b2b84b6test1','Test Testov','+9720000000','2639809','Kiryat Mozkin','Ploshad Mira 15','Kraiot','test@gmail.com');

INSERT INTO payment (order_uid, transaction_id, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
VALUES ('b563feb7b2b84b6test1','b563feb7b2b84b6test','','USD','wbpay',1817,1637907727,'alpha',1500,317,0);

INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
VALUES ('b563feb7b2b84b6test1',9934930,'WBILMTESTTRACK',453,'ab4219087a764ae0btest','Mascaras',30,'0',317,2389212,'Vivienne Sabo',202);
