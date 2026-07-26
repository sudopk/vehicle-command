// Package ble implements the vehicle.Connector interface using BLE via tinygo.org/x/bluetooth.

package ble

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/teslamotors/vehicle-command/internal/log"
	"github.com/teslamotors/vehicle-command/pkg/connector"
	"github.com/teslamotors/vehicle-command/pkg/protocol"
	"tinygo.org/x/bluetooth"
)

const (
	// https://github.com/go-ble/ble/blob/8c5522f543335a80e18fc70e704b104cf3fcc606/const.go#L8
	defaultBLEMTU     = 515
	maxBLEMessageSize = 1024
)

var ErrAdapterInvalidID = protocol.NewError("the bluetooth adapter ID is invalid", false, false)
var ErrMaxConnectionsExceeded = protocol.NewError("the vehicle is already connected to the maximum number of BLE devices", false, false)

var (
	rxTimeout  = time.Second     // Timeout interval between receiving chunks of a message
	maxLatency = 4 * time.Second // Max allowed error when syncing vehicle clock
)

var (
	vehicleServiceUUID bluetooth.UUID
	toVehicleUUID      bluetooth.UUID
	fromVehicleUUID    bluetooth.UUID
)

func init() {
	vehicleServiceUUID, _ = bluetooth.ParseUUID("00000211-b2d1-43f0-9b88-960cebf8b91e")
	toVehicleUUID, _      = bluetooth.ParseUUID("00000212-b2d1-43f0-9b88-960cebf8b91e")
	fromVehicleUUID, _    = bluetooth.ParseUUID("00000213-b2d1-43f0-9b88-960cebf8b91e")
}



var (
	adapter *bluetooth.Adapter
	mu      sync.Mutex
)

type Connection struct {
	vin         string
	inbox       chan []byte
	txChar      bluetooth.DeviceCharacteristic
	blockLength int
	rxChar      bluetooth.DeviceCharacteristic
	inputBuffer []byte
	device      *bluetooth.Device
	lastRx      time.Time
	lock        sync.Mutex
}

func (c *Connection) PreferredAuthMethod() connector.AuthMethod {
	return connector.AuthMethodGCM
}

func (c *Connection) RetryInterval() time.Duration {
	return time.Second
}

func (c *Connection) Receive() <-chan []byte {
	return c.inbox
}

func (c *Connection) flush() bool {
	if len(c.inputBuffer) >= 2 {
		msgLength := 256*int(c.inputBuffer[0]) + int(c.inputBuffer[1])
		if msgLength > maxBLEMessageSize {
			c.inputBuffer = []byte{}
			return false
		}
		if len(c.inputBuffer) >= 2+msgLength {
			buffer := c.inputBuffer[2 : 2+msgLength]
			log.Debug("RX: %02x", buffer)
			c.inputBuffer = c.inputBuffer[2+msgLength:]
			select {
			case c.inbox <- buffer:
			default:
				return false
			}
			return true
		}
	}
	return false
}

func (c *Connection) Close() {
	if c.rxChar.UUID() != (bluetooth.UUID{}) {
		_ = c.rxChar.EnableNotifications(nil)
	}
	if c.device != nil {
		_ = c.device.Disconnect()
	}
}

func (c *Connection) AllowedLatency() time.Duration {
	return maxLatency
}

func (c *Connection) rx(p []byte) {
	if time.Since(c.lastRx) > rxTimeout {
		c.inputBuffer = []byte{}
	}
	c.lastRx = time.Now()
	c.inputBuffer = append(c.inputBuffer, p...)
	for c.flush() {
	}
}

func (c *Connection) Send(_ context.Context, buffer []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var out []byte
	log.Debug("TX: %02x", buffer)
	out = append(out, uint8(len(buffer)>>8), uint8(len(buffer)))
	out = append(out, buffer...)
	blockLength := c.blockLength
	for len(out) > 0 {
		if blockLength > len(out) {
			blockLength = len(out)
		}
		if _, err := c.txChar.WriteWithoutResponse(out[:blockLength]); err != nil {
			if _, err := c.txChar.Write(out[:blockLength]); err != nil {
				return err
			}
		}
		out = out[blockLength:]
	}
	return nil
}

func (c *Connection) VIN() string {
	return c.vin
}

func VehicleLocalName(vin string) string {
	vinBytes := []byte(vin)
	digest := sha1.Sum(vinBytes)
	return fmt.Sprintf("S%02xC", digest[:8])
}

// InitAdapterWithID initializes the BLE adapter with the given ID.
func InitAdapterWithID(id string) error {
	mu.Lock()
	defer mu.Unlock()
	return initAdapter(&id)
}

// CloseAdapter unsets the BLE adapter.
func CloseAdapter() error {
	mu.Lock()
	defer mu.Unlock()
	if adapter != nil {
		_ = adapter.StopScan()
		adapter = nil
		log.Debug("Closed BLE adapter")
	}
	return nil
}

func initAdapter(id *string) error {
	if adapter != nil {
		log.Debug("Reusing existing BLE device")
		return nil
	}
	log.Debug("Creating new BLE adapter")
	ad, err := newAdapter(id)
	if err != nil {
		return fmt.Errorf("ble: failed to enable device: %s", err)
	}
	adapter = ad
	return nil
}

type ScanResult struct {
	Address     string
	LocalName   string
	RSSI        int16
	Connectable bool
}

func ScanVehicleBeacon(ctx context.Context, vin string) (*ScanResult, error) {
	mu.Lock()
	defer mu.Unlock()

	if err := initAdapter(nil); err != nil {
		return nil, err
	}

	a, err := scanVehicleBeacon(ctx, VehicleLocalName(vin))
	if err != nil {
		return nil, fmt.Errorf("ble: failed to scan for %s: %s", vin, err)
	}
	return a, nil
}

func scanVehicleBeacon(ctx context.Context, localName string) (*ScanResult, error) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan *ScanResult, 1)

	// Launch a watcher goroutine to call adapter.StopScan() when ctx is canceled or target is found.
	// adapter.Scan() is blocking, so StopScan() must be called asynchronously to unblock it.
	stopWatcher := make(chan struct{})
	go func() {
		select {
		case <-ctx2.Done():
			if adapter != nil {
				_ = adapter.StopScan()
			}
		case <-stopWatcher:
		}
	}()
	defer close(stopWatcher)

	err := adapter.Scan(func(_ *bluetooth.Adapter, result bluetooth.ScanResult) {
		if result.LocalName() == localName {
			res := &ScanResult{
				Address:     result.Address.String(),
				LocalName:   result.LocalName(),
				RSSI:        int16(result.RSSI),
				Connectable: true,
			}
			select {
			case ch <- res:
				cancel() // Triggers watcher to call StopScan() and unblock adapter.Scan()
			case <-ctx2.Done():
			}
		}
	})

	select {
	case res := <-ch:
		return res, nil
	default:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, err
		}
		select {
		case res := <-ch:
			return res, nil
		default:
			return nil, fmt.Errorf("scan ended without finding vehicle")
		}
	}
}


func NewConnection(ctx context.Context, vin string) (*Connection, error) {
	return NewConnectionFromScanResult(ctx, vin, nil)
}

// NewConnectionFromScanResult creates a new BLE connection to the given target.
func NewConnectionFromScanResult(ctx context.Context, vin string, target *ScanResult) (*Connection, error) {
	var lastError error
	for {
		conn, retry, err := tryToConnect(ctx, vin, target)
		if err == nil {
			return conn, nil
		}
		if !retry || IsAdapterError(err) {
			return nil, err
		}
		log.Warning("BLE connection attempt failed: %s", err)
		if err := ctx.Err(); err != nil {
			if lastError != nil {
				return nil, lastError
			}
			return nil, err
		}
		lastError = err
	}
}

func tryToConnect(ctx context.Context, vin string, target *ScanResult) (*Connection, bool, error) {
	var err error
	mu.Lock()
	defer mu.Unlock()

	if err = initAdapter(nil); err != nil {
		return nil, false, err
	}

	localName := VehicleLocalName(vin)

	if target == nil {
		target, err = scanVehicleBeacon(ctx, localName)
		if err != nil {
			return nil, true, fmt.Errorf("ble: failed to scan for %s: %s", vin, err)
		}
	}

	if target.LocalName != localName {
		return nil, false, fmt.Errorf("ble: beacon with unexpected local name: '%s'", target.LocalName)
	}

	if !target.Connectable {
		return nil, false, ErrMaxConnectionsExceeded
	}

	mac, err := bluetooth.ParseMAC(target.Address)
	if err != nil {
		return nil, false, fmt.Errorf("ble: invalid address %s: %s", target.Address, err)
	}

	log.Debug("Dialing to %s (%s)...", target.Address, localName)
	device, err := adapter.Connect(bluetooth.Address{MACAddress: bluetooth.MACAddress{MAC: mac}}, bluetooth.ConnectionParams{})
	if err != nil {
		return nil, true, fmt.Errorf("ble: failed to dial for %s (%s): %s", vin, localName, err)
	}

	log.Debug("Discovering services %s...", target.Address)
	services, err := device.DiscoverServices([]bluetooth.UUID{vehicleServiceUUID})
	if err != nil || len(services) == 0 {
		_ = device.Disconnect()
		return nil, true, fmt.Errorf("ble: failed to discover service: %v", err)
	}

	characteristics, err := services[0].DiscoverCharacteristics([]bluetooth.UUID{toVehicleUUID, fromVehicleUUID})
	if err != nil {
		_ = device.Disconnect()
		return nil, true, fmt.Errorf("ble: failed to discover service characteristics: %s", err)
	}

	conn := Connection{
		vin:    vin,
		device: &device,
		inbox:  make(chan []byte, 5),
	}

	for _, char := range characteristics {
		if char.UUID() == toVehicleUUID {
			conn.txChar = char
		} else if char.UUID() == fromVehicleUUID {
			conn.rxChar = char
		}
	}

	if conn.txChar.UUID() == (bluetooth.UUID{}) || conn.rxChar.UUID() == (bluetooth.UUID{}) {
		_ = device.Disconnect()
		return nil, true, fmt.Errorf("ble: failed to find required characteristics")
	}

	if err := conn.rxChar.EnableNotifications(conn.rx); err != nil {
		_ = device.Disconnect()
		return nil, true, fmt.Errorf("ble: failed to subscribe to RX: %s", err)
	}

	conn.blockLength = defaultBLEMTU - 3 // 3 bytes header
	log.Info("Connected to vehicle BLE")
	return &conn, false, nil
}
