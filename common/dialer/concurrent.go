package dialer

import (
	"context"
	"net"
	"net/netip"

	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// OwnBox: concurrent dial.
//
// When route.default_concurrent_dial is enabled, every address resolved from
// DNS is dialed at the same time and the first established connection wins,
// matching mihomo's tcp-concurrent behaviour. Both TCP connections and UDP
// packet connections are covered, for outbound destination addresses as well
// as remote (proxy server) addresses.

type concurrentDialer interface {
	ConcurrentDial() bool
}

func concurrentDialEnabled(dialer N.Dialer) bool {
	concurrent, loaded := dialer.(concurrentDialer)
	return loaded && concurrent.ConcurrentDial()
}

type concurrentDialResult struct {
	net.Conn
	error
}

// DialConcurrent dials all destination addresses concurrently, the first success wins.
func DialConcurrent(ctx context.Context, dialer N.Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr) (net.Conn, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			return nil, E.New("missing destination address")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if len(destinationAddresses) == 1 {
		return dialer.DialContext(ctx, network, M.SocksaddrFrom(destinationAddresses[0], destination.Port))
	}
	results := make(chan concurrentDialResult) // unbuffered
	returned := make(chan struct{})
	defer close(returned)
	for _, destinationAddress := range destinationAddresses {
		go func(destinationAddress netip.Addr) {
			conn, err := dialer.DialContext(ctx, network, M.SocksaddrFrom(destinationAddress, destination.Port))
			select {
			case results <- concurrentDialResult{Conn: conn, error: err}:
			case <-returned:
				if conn != nil {
					conn.Close()
				}
			}
		}(destinationAddress)
	}
	var connErrors []error
	for i := 0; i < len(destinationAddresses); i++ {
		result := <-results
		if result.error == nil {
			return result.Conn, nil
		}
		connErrors = append(connErrors, result.error)
	}
	return nil, E.Errors(connErrors...)
}

type concurrentPacketResult struct {
	net.PacketConn
	destinationAddress netip.Addr
	error
}

// ListenConcurrent listens on all destination addresses concurrently, the first success wins.
func ListenConcurrent(ctx context.Context, dialer N.Dialer, destination M.Socksaddr, destinationAddresses []netip.Addr) (net.PacketConn, netip.Addr, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			return nil, netip.Addr{}, E.New("missing destination address")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if len(destinationAddresses) == 1 {
		packetConn, err := dialer.ListenPacket(ctx, M.SocksaddrFrom(destinationAddresses[0], destination.Port))
		return packetConn, destinationAddresses[0], err
	}
	results := make(chan concurrentPacketResult) // unbuffered
	returned := make(chan struct{})
	defer close(returned)
	for _, destinationAddress := range destinationAddresses {
		go func(destinationAddress netip.Addr) {
			packetConn, err := dialer.ListenPacket(ctx, M.SocksaddrFrom(destinationAddress, destination.Port))
			select {
			case results <- concurrentPacketResult{PacketConn: packetConn, destinationAddress: destinationAddress, error: err}:
			case <-returned:
				if packetConn != nil {
					packetConn.Close()
				}
			}
		}(destinationAddress)
	}
	var packetErrors []error
	for i := 0; i < len(destinationAddresses); i++ {
		result := <-results
		if result.error == nil {
			return result.PacketConn, result.destinationAddress, nil
		}
		packetErrors = append(packetErrors, result.error)
	}
	return nil, netip.Addr{}, E.Errors(packetErrors...)
}
