package dialer

import (
	"context"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type concurrentTestConn struct {
	address netip.Addr
}

func (c *concurrentTestConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *concurrentTestConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *concurrentTestConn) Close() error                     { return nil }
func (c *concurrentTestConn) LocalAddr() net.Addr              { return nil }
func (c *concurrentTestConn) RemoteAddr() net.Addr             { return nil }
func (c *concurrentTestConn) SetDeadline(time.Time) error      { return nil }
func (c *concurrentTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *concurrentTestConn) SetWriteDeadline(time.Time) error { return nil }

type concurrentTestPacketConn struct {
	net.PacketConn
}

func (c *concurrentTestPacketConn) Close() error { return nil }

type concurrentTestDialer struct {
	delays   map[string]time.Duration
	failures map[string]bool
	enabled  bool

	access      sync.Mutex
	dialCount   int
	listenAddrs []netip.Addr
}

func (d *concurrentTestDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	d.access.Lock()
	d.dialCount++
	d.access.Unlock()
	key := destination.Addr.String()
	if delay, loaded := d.delays[key]; loaded && delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if d.failures[key] {
		return nil, E.New("dial failed for ", key)
	}
	return &concurrentTestConn{address: destination.Addr}, nil
}

func (d *concurrentTestDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	d.access.Lock()
	d.listenAddrs = append(d.listenAddrs, destination.Addr)
	d.access.Unlock()
	return &concurrentTestPacketConn{}, nil
}

func (d *concurrentTestDialer) ConcurrentDial() bool {
	return d.enabled
}

func (d *concurrentTestDialer) listenAddresses() []netip.Addr {
	d.access.Lock()
	defer d.access.Unlock()
	return append([]netip.Addr(nil), d.listenAddrs...)
}

func testAddresses() (netip.Addr, netip.Addr) {
	return netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("2001:db8::1")
}

func TestDialConcurrentPrefersConfiguredFamily(t *testing.T) {
	address4, address6 := testAddresses()
	dialer := &concurrentTestDialer{
		delays: map[string]time.Duration{address6.String(): 20 * time.Millisecond},
	}
	conn, err := DialConcurrent(context.Background(), dialer, N.NetworkTCP, M.ParseSocksaddr("example.com:443"),
		[]netip.Addr{address4, address6}, C.DomainStrategyPreferIPv6, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, address6, conn.(*concurrentTestConn).address)
}

func TestDialConcurrentUsesFallbackAfterPreferredFailure(t *testing.T) {
	address4, address6 := testAddresses()
	dialer := &concurrentTestDialer{
		failures: map[string]bool{address6.String(): true},
	}
	conn, err := DialConcurrent(context.Background(), dialer, N.NetworkTCP, M.ParseSocksaddr("example.com:443"),
		[]netip.Addr{address4, address6}, C.DomainStrategyPreferIPv6, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, address4, conn.(*concurrentTestConn).address)
}

func TestDialConcurrentUsesFallbackAfterPreferredWindow(t *testing.T) {
	address4, address6 := testAddresses()
	dialer := &concurrentTestDialer{
		delays: map[string]time.Duration{address6.String(): 5 * time.Second},
	}
	start := time.Now()
	conn, err := DialConcurrent(context.Background(), dialer, N.NetworkTCP, M.ParseSocksaddr("example.com:443"),
		[]netip.Addr{address4, address6}, C.DomainStrategyPreferIPv6, 30*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, address4, conn.(*concurrentTestConn).address)
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestDialConcurrentFollowsResolvedOrderWithoutStrategy(t *testing.T) {
	address4, address6 := testAddresses()
	dialer := &concurrentTestDialer{}
	conn, err := DialConcurrent(context.Background(), dialer, N.NetworkTCP, M.ParseSocksaddr("example.com:443"),
		[]netip.Addr{address6, address4}, C.DomainStrategyAsIS, 60*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, address6, conn.(*concurrentTestConn).address)
}

func TestListenSerialNetworkPacketStaysSerial(t *testing.T) {
	address4, address6 := testAddresses()
	dialer := &concurrentTestDialer{enabled: true}
	packetConn, address, err := ListenSerialNetworkPacket(context.Background(), dialer, M.ParseSocksaddr("example.com:443"),
		[]netip.Addr{address4, address6}, nil, nil, nil, 0)
	require.NoError(t, err)
	require.Equal(t, address4, address)
	require.Equal(t, []netip.Addr{address4}, dialer.listenAddresses())
	require.NoError(t, packetConn.Close())
}

func TestDialSerialNetworkStillConcurrent(t *testing.T) {
	address4, address6 := testAddresses()
	dialer := &concurrentTestDialer{
		enabled: true,
		delays:  map[string]time.Duration{address4.String(): time.Second},
	}
	start := time.Now()
	conn, err := DialSerialNetwork(context.Background(), dialer, N.NetworkUDP, M.ParseSocksaddr("example.com:443"),
		[]netip.Addr{address4, address6}, nil, nil, nil, 50*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, address6, conn.(*concurrentTestConn).address)
	require.Less(t, time.Since(start), 800*time.Millisecond)
}

func TestDialConcurrentLateFallbackAfterWindowExpiry(t *testing.T) {
	address4, address6 := testAddresses()
	dialer := &concurrentTestDialer{
		delays:   map[string]time.Duration{address4.String(): 100 * time.Millisecond},
		failures: map[string]bool{address6.String(): true},
	}
	conn, err := DialConcurrent(context.Background(), dialer, N.NetworkTCP, M.ParseSocksaddr("example.com:443"),
		[]netip.Addr{address4, address6}, C.DomainStrategyPreferIPv6, 20*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, address4, conn.(*concurrentTestConn).address)
}
