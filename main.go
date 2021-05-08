package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

const (
	heartbeat   = 25 * time.Second
	pingTimeout = 1 * time.Second
)

func WaitForNotification(origCtx context.Context, conn *pgx.Conn) error {
	ctx, cancel := context.WithTimeout(origCtx, heartbeat)
	defer cancel()

	n, err := conn.WaitForNotification(ctx)

	if err == nil {
		log.Printf("[%d] %s: %s\n", n.PID, n.Channel, n.Payload)
	} else if pgconn.Timeout(err) {
		ctx, cancel = context.WithTimeout(origCtx, pingTimeout)
		defer cancel()

		err = conn.Ping(ctx)
	}

	return err
}

func main() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal("Connect error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err = conn.Exec(ctx, "LISTEN foo"); err != nil {
		log.Println("LISTEN query error", err)
		return
	}

	for {
		err := WaitForNotification(ctx, conn)
		if err != nil {
			log.Println("WaitForNotification or Ping error", err)
			return
		}
	}
}
