package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikitos212/go-orders/pkg/model"
	"github.com/segmentio/kafka-go"
)

var (
	cache     = make(map[string]model.Order)
	cacheLock = &sync.RWMutex{}

	cacheExpiry = make(map[string]time.Time)
)

func main() {
	kafkaBrokersEnv := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "orders")
	addr := getEnv("HTTP_ADDR", ":8080")
	dbURL := getEnv("DB_URL", "postgresql://orders_user:secret@localhost:5432/orders_db")
	cacheLoadLimit := atoi(getEnv("CACHE_LOAD_LIMIT", "10"))
	cacheTTLSeconds := atoi(getEnv("CACHE_TTL_SECONDS", "360"))
	cacheJanitorInterval := time.Duration(atoi(getEnv("CACHE_JANITOR_S", "60"))) * time.Second

	brokers := splitAndTrim(kafkaBrokersEnv, ",")

	db, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer db.Close()

	if err := loadRecentOrdersIntoCache(context.Background(), db, cacheLoadLimit); err != nil {
		log.Printf("warning: preload cache failed: %v", err)
	} else {
		log.Printf("cache preloaded with up to %d recent orders", cacheLoadLimit)
	}

	ctxBg, bgCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startCacheJanitor(ctxBg, cacheJanitorInterval)
	}()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.Hash{},
		Async:    false,
	})
	writerLookup := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "order-lookup",
		Balancer: &kafka.Hash{},
		Async:    false,
	})

	readerFound := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    "order-found",
		GroupID:  "api-order-found-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, err := readerFound.FetchMessage(ctxBg)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				log.Printf("order-found reader fetch err: %v", err)
				time.Sleep(time.Second)
				continue
			}
			var ord model.Order
			if err := json.Unmarshal(msg.Value, &ord); err != nil {
				log.Printf("order-found unmarshal error: %v", err)
				_ = readerFound.CommitMessages(ctxBg, msg)
				continue
			}

			setCache(ord, time.Duration(cacheTTLSeconds)*time.Second)

			log.Printf("cache updated from order-found for %s", ord.OrderUID)
			if err := readerFound.CommitMessages(ctxBg, msg); err != nil {
				log.Printf("commit order-found failed: %v", err)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/orders", makeOrdersHandler(writer))
	mux.HandleFunc("/api/order/", makeGetHandler(writerLookup))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	cacheLock.Lock()
	cache["b563feb7b2b84b6test"] = model.Order{
		OrderUID:    "b563feb7b2b84b6test",
		TrackNumber: "WBILMTESTTRACK",
		Entry:       "WBIL",
		Locale:      "en",
		CustomerID:  "test",
	}
	cacheExpiry["b563feb7b2b84b6test"] = time.Now().Add(time.Duration(cacheTTLSeconds) * time.Second)
	cacheLock.Unlock()

	fs := http.FileServer(http.Dir("web"))
	mux.Handle("/", fs)

	srv := &http.Server{
		Addr:    addr,
		Handler: enableCORS(loggingMiddleware(mux)),
	}

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdownCh
		log.Println("shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)

		bgCancel()
		wg.Wait()

		if err := readerFound.Close(); err != nil {
			log.Printf("readerFound close error: %v", err)
		}
		if err := writerLookup.Close(); err != nil {
			log.Printf("writerLookup close error: %v", err)
		}
		if err := writer.Close(); err != nil {
			log.Printf("writer close error: %v", err)
		}
	}()

	log.Printf("API listening on %s, Kafka brokers=%v, topic=%s", addr, brokers, topic)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("ListenAndServe: %v", err)
	}

	log.Println("server stopped")
}

func setCache(o model.Order, ttl time.Duration) {
	cacheLock.Lock()
	cache[o.OrderUID] = o
	if ttl > 0 {
		cacheExpiry[o.OrderUID] = time.Now().Add(ttl)
	} else {
		delete(cacheExpiry, o.OrderUID)
	}
	cacheLock.Unlock()
}

func getCache(uid string) (model.Order, bool) {
	cacheLock.RLock()
	exp, hasExp := cacheExpiry[uid]
	if hasExp && time.Now().After(exp) {
		cacheLock.RUnlock()
		cacheLock.Lock()
		delete(cache, uid)
		delete(cacheExpiry, uid)
		cacheLock.Unlock()
		return model.Order{}, false
	}
	o, ok := cache[uid]
	cacheLock.RUnlock()
	return o, ok
}

func startCacheJanitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			cacheLock.Lock()
			for k, exp := range cacheExpiry {
				if now.After(exp) {
					delete(cacheExpiry, k)
					delete(cache, k)
				}
			}
			cacheLock.Unlock()
		}
	}
}

func loadRecentOrdersIntoCache(ctx context.Context, db *pgxpool.Pool, limit int) error {
	if limit <= 0 {
		return nil
	}
	rows, err := db.Query(ctx, `
SELECT order_uid FROM orders
ORDER BY date_created DESC
LIMIT $1
`, limit)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			log.Printf("cache preload scan error: %v", err)
			continue
		}
		ord, err := fetchOrderFromDB(ctx, db, uid)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			log.Printf("cache preload fetch error for %s: %v", uid, err)
			continue
		}
		setCache(*ord, time.Duration(atoi(getEnv("CACHE_TTL_SECONDS", "3600")))*time.Second)
	}
	return nil
}

func fetchOrderFromDB(ctx context.Context, db *pgxpool.Pool, id string) (*model.Order, error) {
	var o model.Order
	var dateCreated time.Time

	row := db.QueryRow(ctx, `
SELECT order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
FROM orders WHERE order_uid=$1
`, id)
	if err := row.Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature, &o.CustomerID, &o.DeliveryService, &o.ShardKey, &o.SmID, &dateCreated, &o.OofShard); err != nil {
		return nil, err
	}
	o.DateCreated = dateCreated.UTC().Format(time.RFC3339)

	row = db.QueryRow(ctx, `SELECT name, phone, zip, city, address, region, email FROM delivery WHERE order_uid=$1`, id)
	var d model.Delivery
	if err := row.Scan(&d.Name, &d.Phone, &d.Zip, &d.City, &d.Address, &d.Region, &d.Email); err == nil {
		o.Delivery = d
	} else if err != pgx.ErrNoRows {
		log.Printf("warning: delivery scan %s: %v", id, err)
	}

	row = db.QueryRow(ctx, `SELECT transaction_id, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee FROM payment WHERE order_uid=$1`, id)
	var p model.Payment
	if err := row.Scan(&p.Transaction, &p.RequestID, &p.Currency, &p.Provider, &p.Amount, &p.PaymentDt, &p.Bank, &p.DeliveryCost, &p.GoodsTotal, &p.CustomFee); err == nil {
		o.Payment = p
	} else if err != pgx.ErrNoRows {
		log.Printf("warning: payment scan %s: %v", id, err)
	}

	rows, err := db.Query(ctx, `SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status FROM items WHERE order_uid=$1 ORDER BY id`, id)
	if err == nil {
		defer rows.Close()
		items := make([]model.Item, 0)
		for rows.Next() {
			var it model.Item
			if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.Rid, &it.Name, &it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
				log.Printf("warning: items scan %s: %v", id, err)
				continue
			}
			items = append(items, it)
		}
		if len(items) > 0 {
			o.Items = items
		}
	} else {
		log.Printf("warning: items query %s: %v", id, err)
	}

	return &o, nil
}

func makeOrdersHandler(writer *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var ord model.Order
		if err := json.NewDecoder(r.Body).Decode(&ord); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if ord.OrderUID == "" {
			http.Error(w, "missing order_uid", http.StatusBadRequest)
			return
		}

		value, err := json.Marshal(ord)
		if err != nil {
			http.Error(w, "failed to marshal order", http.StatusInternalServerError)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		msg := kafka.Message{
			Key:   []byte(ord.OrderUID),
			Value: value,
		}

		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Printf("kafka write error: %v", err)
			http.Error(w, "failed to dispatch order", http.StatusInternalServerError)
			return
		}

		setCache(ord, time.Duration(atoi(getEnv("CACHE_TTL_SECONDS", "3600")))*time.Second)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":    "accepted",
			"order_uid": ord.OrderUID,
		})
	}
}

func makeGetHandler(writerLookup *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/order/")
		orderUID := strings.Split(path, "/")[0]
		if orderUID == "" {
			http.Error(w, "Missing order_uid", http.StatusBadRequest)
			return
		}

		if order, ok := getCache(orderUID); ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(order)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		lookupMsg := kafka.Message{
			Key:   []byte(orderUID),
			Value: []byte(orderUID),
			Time:  time.Now(),
		}

		if err := writerLookup.WriteMessages(ctx, lookupMsg); err != nil {
			log.Printf("failed to send lookup for %s: %v", orderUID, err)
			http.Error(w, "failed to queue lookup", http.StatusInternalServerError)
			return
		}

		waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer waitCancel()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-waitCtx.Done():
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"status":    "lookup_queued",
					"order_uid": orderUID,
				})
				return
			case <-ticker.C:
				if order, ok := getCache(orderUID); ok {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(order)
					return
				}
			}
		}
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func atoi(s string) int {
	i, _ := strconvAtoi(s)
	return i
}

func strconvAtoi(s string) (int, error) {
	return strconv.Atoi(s)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
