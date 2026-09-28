package dialer

import (
	"context"
	"net"
	"net/netip"
	"time"

	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// OwnBox: concurrent dial.
//
// When route.default_concurrent_dial is enabled, every address resolved from
// DNS is dialed at the same time and the first established connection wins,
// matching mihomo's tcp-concurrent behaviour. Only connection-style dials
// (TCP and connected UDP) are covered; session packet listeners stay serial,
// matching mihomo, where ListenPacket never races.
//
// When the resolver strategy carries an address family preference, a success
// from the non-preferred family is held as a fallback and is only used when the
// preferred family fails or fallbackDelay (300ms by default) elapses, matching
// mihomo's dual-stack behaviour.

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
	address netip.Addr
}

// DialConcurrent dials all destination addresses concurrently, the first success wins.
// strategy carries the resolver's address family preference; when both families are
// present, the non-preferred family can only win after fallbackDelay, or when the
// preferred family fails.
func DialConcurrent(ctx context.Context, dialer N.Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy C.DomainStrategy, fallbackDelay time.Duration) (net.Conn, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			return nil, E.New("missing destination address")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if len(destinationAddresses) == 1 {
		return dialer.DialContext(ctx, network, M.SocksaddrFrom(destinationAddresses[0], destination.Port))
	}
	preferredFamily := concurrentPreferredAddressFamily(strategy, destinationAddresses)
	if fallbackDelay <= 0 {
		fallbackDelay = N.DefaultFallbackDelay
	}
	results := make(chan concurrentDialResult) // unbuffered
	returned := make(chan struct{})
	defer close(returned)
	for _, destinationAddress := range destinationAddresses {
		go func(destinationAddress netip.Addr) {
			conn, err := dialer.DialContext(ctx, network, M.SocksaddrFrom(destinationAddress, destination.Port))
			select {
			case results <- concurrentDialResult{Conn: conn, error: err, address: destinationAddress}:
			case <-returned:
				if conn != nil {
					conn.Close()
				}
			}
		}(destinationAddress)
	}
	var (
		fallbackConn  net.Conn
		fallbackTimer = time.NewTimer(fallbackDelay)
		windowExpired bool
		connErrors    []error
	)
	defer fallbackTimer.Stop()
	pending := len(destinationAddresses)
	for pending > 0 {
		select {
		case result := <-results:
			pending--
			if result.error != nil {
				connErrors = append(connErrors, result.error)
				continue
			}
			if preferredFamily == 0 || addressFamily(result.address) == preferredFamily {
				if fallbackConn != nil {
					fallbackConn.Close()
				}
				return result.Conn, nil
			}
			if windowExpired {
				return result.Conn, nil
			}
			if fallbackConn == nil {
				fallbackConn = result.Conn
			} else {
				result.Conn.Close()
			}
		case <-fallbackTimer.C:
			windowExpired = true
			if fallbackConn != nil {
				return fallbackConn, nil
			}
		}
	}
	if fallbackConn != nil {
		return fallbackConn, nil
	}
	return nil, E.Errors(connErrors...)
}

// concurrentPreferredAddressFamily returns the preferred family for the concurrent race,
// or 0 when no family should be preferred. Without an explicit strategy the resolver
// already ordered the addresses with the preferred family first.
func concurrentPreferredAddressFamily(strategy C.DomainStrategy, destinationAddresses []netip.Addr) int {
	family := 0
	switch strategy {
	case C.DomainStrategyPreferIPv6, C.DomainStrategyIPv6Only:
		family = 6
	case C.DomainStrategyPreferIPv4, C.DomainStrategyIPv4Only:
		family = 4
	default:
		if len(destinationAddresses) > 0 {
			family = addressFamily(destinationAddresses[0])
		}
	}
	hasPreferred, hasFallback := false, false
	for _, address := range destinationAddresses {
		if addressFamily(address) == family {
			hasPreferred = true
		} else {
			hasFallback = true
		}
	}
	if !hasPreferred || !hasFallback {
		return 0
	}
	return family
}

func addressFamily(address netip.Addr) int {
	if address.Is4() || address.Is4In6() {
		return 4
	}
	return 6
}
