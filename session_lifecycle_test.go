package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow"
)

func TestSessionConnectRetry(t *testing.T) {
	failure := errors.New("temporary connection failure")
	for _, successAt := range []int{1, 2, 3, 0} {
		t.Run(map[int]string{1: "first attempt", 2: "second attempt", 3: "last attempt", 0: "exhausted"}[successAt], func(t *testing.T) {
			calls := 0
			err := connectSessionWithRetry(context.Background(), func(context.Context) error {
				calls++
				if calls == successAt {
					return nil
				}
				return failure
			}, 3, 0)
			if successAt == 0 {
				if !errors.Is(err, failure) || calls != 3 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			} else if err != nil || calls != successAt {
				t.Fatalf("successful retry treated as failure: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestSessionQREvents(t *testing.T) {
	t.Run("events remain ordered until channel closes", func(t *testing.T) {
		events := make(chan whatsmeow.QRChannelItem, 2)
		events <- whatsmeow.QRChannelItem{Event: "code", Code: "synthetic-test-code"}
		events <- whatsmeow.QRChannelSuccess
		close(events)
		for _, want := range []string{"code", "success"} {
			evt, ok := nextSessionQREvent(context.Background(), events)
			if !ok || evt.Event != want {
				t.Fatalf("event=%q open=%v, want %q", evt.Event, ok, want)
			}
			if want == "code" && evt.Code != "synthetic-test-code" {
				t.Fatal("QR event payload changed")
			}
		}
		if _, ok := nextSessionQREvent(context.Background(), events); ok {
			t.Fatal("closed channel produced an event")
		}
	})

	t.Run("canceled session ignores buffered events", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		events := make(chan whatsmeow.QRChannelItem, 1)
		events <- whatsmeow.QRChannelItem{Event: "code"}
		if _, ok := nextSessionQREvent(ctx, events); ok {
			t.Fatal("canceled session accepted a stale QR event")
		}
		if len(events) != 1 {
			t.Fatal("canceled session consumed a pending QR event")
		}
	})
}

func TestSessionQRCancellationWithOpenChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan whatsmeow.QRChannelItem)
	defer close(events) // Also releases the old blocking implementation on failure.
	started := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		close(started)
		_, ok := nextSessionQREvent(ctx, events)
		done <- ok
	}()
	<-started
	select {
	case <-done:
		t.Fatal("QR wait ended before cancellation")
	default:
	}
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("cancellation returned an event")
		}
	case <-time.After(time.Second):
		t.Fatal("session remained blocked on an open QR channel after cancellation")
	}
}

func TestSessionQRShutdownReleasesOnlyItsReservation(t *testing.T) {
	startOwner := func(id string) (chan whatsmeow.QRChannelItem, <-chan struct{}) {
		t.Helper()
		kill := make(chan bool, 1)
		setKillChannel(id, kill)
		ctx, cancel := sessionContext(kill)
		events := make(chan whatsmeow.QRChannelItem)
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer deleteKillChannel(id, kill)
			defer cancel()
			for {
				if _, ok := nextSessionQREvent(ctx, events); !ok {
					return
				}
			}
		}()
		t.Cleanup(func() {
			cancel()
			close(events)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("test session owner did not exit")
			}
		})
		return events, done
	}
	idA, idB := t.Name()+"-a", t.Name()+"-b"
	_, doneA := startOwner(idA)
	eventsB, doneB := startOwner(idB)
	signalKill(idA)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitForSessionStop(ctx, idA); err != nil {
		t.Fatal("shutdown did not release the QR session reservation:", err)
	}
	<-doneA
	if _, registered := getKillChannel(idB); !registered {
		t.Fatal("shutdown removed another session's reservation")
	}
	select {
	case <-doneB:
		t.Fatal("shutdown canceled another session")
	default:
	}
	select {
	case eventsB <- whatsmeow.QRChannelItem{Event: "code"}:
	case <-time.After(time.Second):
		t.Fatal("other session stopped accepting QR events")
	}
	signalKill(idB)
	ctxB, cancelB := context.WithTimeout(context.Background(), time.Second)
	defer cancelB()
	if err := waitForSessionStop(ctxB, idB); err != nil {
		t.Fatal("second session did not shut down independently:", err)
	}
}

func TestSessionConnectRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := connectSessionWithRetry(ctx, func(context.Context) error {
		calls++
		cancel()
		return errors.New("dial interrupted")
	}, 3, time.Hour)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestSessionCleanupPreservesReconnectState(t *testing.T) {
	db, err := sqlx.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY, connected INTEGER, qrcode TEXT, events TEXT);
 INSERT INTO users VALUES ('session', 1, 'old QR', 'Message')`); err != nil {
		t.Fatal(err)
	}
	s := &server{db: db}
	check := func(want int) {
		t.Helper()
		var connected int
		var qr, events string
		if err := db.QueryRow(`SELECT connected,qrcode,events FROM users WHERE id='session'`).Scan(&connected, &qr, &events); err != nil {
			t.Fatal(err)
		}
		if connected != want || qr != "" || events != "Message" {
			t.Fatalf("connected=%d qr=%q events=%q", connected, qr, events)
		}
	}
	// Failed startup connection must remain eligible for the next restart.
	if err := s.clearSessionQRCode("session"); err != nil {
		t.Fatal(err)
	}
	check(1)
	// Explicit disconnect/logout disables reconnect, and cleanup must preserve it.
	if err := s.setDisconnectedState("session", false); err != nil {
		t.Fatal(err)
	}
	if err := s.clearSessionQRCode("session"); err != nil {
		t.Fatal(err)
	}
	check(0)
}
