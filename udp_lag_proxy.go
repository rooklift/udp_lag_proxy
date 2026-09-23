// lagproxy: a UDP man-in-the-middle that delays traffic in both directions.
//
// Usage:
//	go run . -listen :27501 -target 127.0.0.1:27500 -delay 100ms
// then point the client at port 27501 instead of 27500.

package main

import (
	"flag"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

var (
	listen_addr = flag.String("listen", ":27501", "address the proxy listens on (clients connect here)")
	target_addr = flag.String("target", "127.0.0.1:27500", "address of the real server")
	delay       = flag.Duration("delay", 100 * time.Millisecond, "one-way delay applied in each direction")
	jitter      = flag.Duration("jitter", 0, "random +/- variation added to the delay")
	loss        = flag.Float64("loss", 0, "probability (0..1) of dropping each packet")
)

type delayed_packet struct {
	data []byte
	due  time.Time
}

// delay_line returns a channel; packets sent into it are passed to send() once
// their due time arrives. Packets are handled strictly in FIFO order, so jitter
// never reorders packets (a late packet holds up the ones behind it).
func delay_line(send func([]byte)) chan<- delayed_packet {
	ch := make(chan delayed_packet, 4096)
	go func() {
		for pkt := range ch {
			time.Sleep(time.Until(pkt.due))
			send(pkt.data)
		}
	}()
	return ch
}

func pick_delay() time.Duration {
	d := *delay
	if *jitter > 0 {
		span := int64(*jitter) * 2 + 1
		d += time.Duration(rand.Int63n(span)) - *jitter
	}
	if d < 0 {
		d = 0
	}
	return d
}

func should_drop() bool {
	return *loss > 0 && rand.Float64() < *loss
}

type session struct {
	to_server chan<- delayed_packet
}

func new_session(listener *net.UDPConn, client_addr *net.UDPAddr, target *net.UDPAddr) (*session, error) {
	upstream, err := net.DialUDP("udp", nil, target)
	if err != nil {
		return nil, err
	}

	s := &session{}

	s.to_server = delay_line(func(b []byte) {
		if _, err := upstream.Write(b); err != nil {
			log.Printf("[%v] write to server: %v", client_addr, err)
		}
	})

	to_client := delay_line(func(b []byte) {
		if _, err := listener.WriteToUDP(b, client_addr); err != nil {
			log.Printf("[%v] write to client: %v", client_addr, err)
		}
	})

	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := upstream.Read(buf)
			if err != nil {
				// e.g. "connection refused" if the server isn't up yet
				log.Printf("[%v] read from server: %v", client_addr, err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if should_drop() {
				continue
			}
			data := append([]byte(nil), buf[:n]...)
			to_client <- delayed_packet{data, time.Now().Add(pick_delay())}
		}
	}()

	return s, nil
}

func main() {
	flag.Parse()

	target, err := net.ResolveUDPAddr("udp", *target_addr)
	if err != nil {
		log.Fatalf("bad -target: %v", err)
	}
	laddr, err := net.ResolveUDPAddr("udp", *listen_addr)
	if err != nil {
		log.Fatalf("bad -listen: %v", err)
	}
	listener, err := net.ListenUDP("udp", laddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	log.Printf("proxying %v -> %v, delay %v, jitter %v, loss %.1f%%",
		listener.LocalAddr(), target, *delay, *jitter, *loss * 100)

	var mu sync.Mutex
	sessions := map[string]*session{}
	buf := make([]byte, 65536)

	for {
		n, client_addr, err := listener.ReadFromUDP(buf)
		if err != nil {
			log.Printf("read from client: %v", err)
			continue
		}

		key := client_addr.String()
		mu.Lock()
		s := sessions[key]
		if s == nil {
			s, err = new_session(listener, client_addr, target)
			if err != nil {
				mu.Unlock()
				log.Printf("[%v] new session: %v", client_addr, err)
				continue
			}
			sessions[key] = s
			log.Printf("[%v] new client", client_addr)
		}
		mu.Unlock()

		if should_drop() {
			continue
		}
		data := append([]byte(nil), buf[:n]...)
		s.to_server <- delayed_packet{data, time.Now().Add(pick_delay())}
	}
}
