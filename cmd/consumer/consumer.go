package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikitos212/go-orders/pkg/model"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaBroker = "localhost:9092"
	topic       = "orders"
	groupID     = "order-service-group"
	dbURL       = "postgresql://orders_user:secret@localhost:5432/orders_db"
)

func GetOrderFromDB(ctx context.Context, db *pgxpool.Pool, id string) (*model.Order, error) {
	var o model.Order

	var dateCreated time.Time
	row := db.QueryRow(ctx, `
SELECT order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
FROM orders WHERE order_uid = $1
`, id)

	err := row.Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature, &o.CustomerID, &o.DeliveryService, &o.ShardKey, &o.SmID, &dateCreated, &o.OofShard)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, pgx.ErrNoRows
		}
		return nil, err
	}
	o.DateCreated = dateCreated.UTC().Format(time.RFC3339)

	row = db.QueryRow(ctx, `SELECT name, phone, zip, city, address, region, email FROM delivery WHERE order_uid=$1`, id)
	var d model.Delivery
	if err := row.Scan(&d.Name, &d.Phone, &d.Zip, &d.City, &d.Address, &d.Region, &d.Email); err == nil {
		o.Delivery = d
	} else {
		if err != pgx.ErrNoRows {
			log.Printf("warning: delivery scan for %s: %v", id, err)
		}
	}

	row = db.QueryRow(ctx, `SELECT transaction_id, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee FROM payment WHERE order_uid=$1`, id)
	var p model.Payment
	if err := row.Scan(&p.Transaction, &p.RequestID, &p.Currency, &p.Provider, &p.Amount, &p.PaymentDt, &p.Bank, &p.DeliveryCost, &p.GoodsTotal, &p.CustomFee); err == nil {
		o.Payment = p
	} else {
		if err != pgx.ErrNoRows {
			log.Printf("warning: payment scan for %s: %v", id, err)
		}
	}

	rows, err := db.Query(ctx, `SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status FROM items WHERE order_uid=$1 ORDER BY id`, id)
	if err != nil {
		return &o, err
	}
	defer rows.Close()

	items := make([]model.Item, 0)
	for rows.Next() {
		var it model.Item
		if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.Rid, &it.Name, &it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
			log.Printf("warning: items scan for %s: %v", id, err)
			continue
		}
		items = append(items, it)
	}
	if len(items) > 0 {
		o.Items = items
	}

	return &o, nil
}

func SaveOrderToDB(ctx context.Context, db *pgxpool.Pool, o *model.Order) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
INSERT INTO orders(order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (order_uid) DO UPDATE SET
  track_number=EXCLUDED.track_number,
  entry=EXCLUDED.entry,
  locale=EXCLUDED.locale,
  internal_signature=EXCLUDED.internal_signature,
  customer_id=EXCLUDED.customer_id,
  delivery_service=EXCLUDED.delivery_service,
  shardkey=EXCLUDED.shardkey,
  sm_id=EXCLUDED.sm_id,
  date_created=EXCLUDED.date_created,
  oof_shard=EXCLUDED.oof_shard;
`, o.OrderUID, o.TrackNumber, o.Entry, o.Locale, o.InternalSignature, o.CustomerID, o.DeliveryService, o.ShardKey, o.SmID, o.DateCreated, o.OofShard)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
INSERT INTO delivery(order_uid, name, phone, zip, city, address, region, email)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (order_uid) DO UPDATE SET
  name=EXCLUDED.name, phone=EXCLUDED.phone, zip=EXCLUDED.zip, city=EXCLUDED.city,
  address=EXCLUDED.address, region=EXCLUDED.region, email=EXCLUDED.email;
`, o.OrderUID, o.Delivery.Name, o.Delivery.Phone, o.Delivery.Zip, o.Delivery.City, o.Delivery.Address, o.Delivery.Region, o.Delivery.Email)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
INSERT INTO payment(order_uid, transaction_id, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (order_uid) DO UPDATE SET
  transaction_id=EXCLUDED.transaction_id, request_id=EXCLUDED.request_id, currency=EXCLUDED.currency,
  provider=EXCLUDED.provider, amount=EXCLUDED.amount, payment_dt=EXCLUDED.payment_dt, bank=EXCLUDED.bank,
  delivery_cost=EXCLUDED.delivery_cost, goods_total=EXCLUDED.goods_total, custom_fee=EXCLUDED.custom_fee;
`, o.OrderUID, o.Payment.Transaction, o.Payment.RequestID, o.Payment.Currency, o.Payment.Provider, o.Payment.Amount, o.Payment.PaymentDt, o.Payment.Bank, o.Payment.DeliveryCost, o.Payment.GoodsTotal, o.Payment.CustomFee)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM items WHERE order_uid=$1`, o.OrderUID)
	if err != nil {
		return err
	}

	for _, it := range o.Items {
		_, err = tx.Exec(ctx, `
INSERT INTO items(order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
`, o.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.Rid, it.Name, it.Sale, it.Size, it.TotalPrice, it.NmID, it.Brand, it.Status)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func main() {
	ctx := context.Background()

	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer db.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer r.Close()

	lookupReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "order-lookup",
		GroupID:  "order-lookup-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer lookupReader.Close()

	foundWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "order-found",
		Balancer: &kafka.Hash{},
	})
	defer foundWriter.Close()

	go func() {
		for {
			msg, err := lookupReader.FetchMessage(ctx)
			if err != nil {
				log.Printf("lookup fetch error: %v", err)
				time.Sleep(time.Second)
				continue
			}

			orderUID := string(msg.Value)
			ord, err := GetOrderFromDB(ctx, db, orderUID)
			if err != nil {
				if err == pgx.ErrNoRows {
					log.Printf("lookup: not found %s", orderUID)
				} else {
					log.Printf("lookup DB error for %s: %v", orderUID, err)
				}
				_ = lookupReader.CommitMessages(ctx, msg)
				continue
			}

			b, _ := json.Marshal(ord)
			respMsg := kafka.Message{
				Key:   []byte(ord.OrderUID),
				Value: b,
				Time:  time.Now(),
			}
			if err := foundWriter.WriteMessages(ctx, respMsg); err != nil {
				log.Printf("failed to write found msg for %s: %v", ord.OrderUID, err)
			} else {
				log.Printf("published order-found for %s", ord.OrderUID)
			}

			if err := lookupReader.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit lookup failed: %v", err)
			}
		}
	}()

	log.Println("consumer started")
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var ord model.Order
		if err := json.Unmarshal(msg.Value, &ord); err != nil {
			log.Printf("invalid message offset=%d: %v", msg.Offset, err)
			if err2 := r.CommitMessages(ctx, msg); err2 != nil {
				log.Printf("commit invalid msg failed: %v", err2)
			}
			continue
		}

		if err := SaveOrderToDB(ctx, db, &ord); err != nil {
			log.Printf("save to db failed for %s: %v", ord.OrderUID, err)
			continue
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit offset failed for %d: %v", msg.Offset, err)
		} else {
			log.Printf("processed order %s (offset %d)", ord.OrderUID, msg.Offset)
		}
	}
}
