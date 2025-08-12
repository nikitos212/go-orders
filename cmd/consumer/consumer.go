package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaBroker = "localhost:9092"
	topic       = "orders"
	groupID     = "order-service-group"
	dbURL       = "postgresql://orders_user:secret@localhost:5432/orders_db"
)

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int64  `json:"amount"`
	PaymentDt    int64  `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int64  `json:"delivery_cost"`
	GoodsTotal   int64  `json:"goods_total"`
	CustomFee    int64  `json:"custom_fee"`
}

type Item struct {
	ChrtID      int64  `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int64  `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int64  `json:"total_price"`
	NmID        int64  `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

type Order struct {
	OrderUID          string   `json:"order_uid"`
	TrackNumber       string   `json:"track_number"`
	Entry             string   `json:"entry"`
	Delivery          Delivery `json:"delivery"`
	Payment           Payment  `json:"payment"`
	Items             []Item   `json:"items"`
	Locale            string   `json:"locale"`
	InternalSignature string   `json:"internal_signature"`
	CustomerID        string   `json:"customer_id"`
	DeliveryService   string   `json:"delivery_service"`
	ShardKey          string   `json:"shardkey"`
	SmID              int      `json:"sm_id"`
	DateCreated       string   `json:"date_created"`
	OofShard          string   `json:"oof_shard"`
}

func SaveOrderToDB(ctx context.Context, db *pgxpool.Pool, o *Order) error {
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

	log.Println("consumer started")
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var ord Order
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
