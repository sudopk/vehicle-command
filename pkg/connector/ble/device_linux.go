package ble

import (
	"fmt"
	"strings"

	"tinygo.org/x/bluetooth"
)

func IsAdapterError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "bluetooth") ||
		strings.Contains(errStr, "bluez") ||
		strings.Contains(errStr, "adapter") ||
		strings.Contains(errStr, "operation not permitted")
}

func AdapterErrorHelpMessage(err error) string {
	return "Failed to initialize BlueZ BLE adapter: \n\t" + err.Error() + "\n" +
		"Please ensure bluetoothd is running and your user has access to Bluetooth.\n"
}

func newAdapter(id *string) (*bluetooth.Adapter, error) {
	var ad *bluetooth.Adapter
	if id != nil && *id != "" {
		if !strings.HasPrefix(*id, "hci") {
			return nil, ErrAdapterInvalidID
		}
		ad = bluetooth.NewAdapter(*id)
	} else {
		ad = bluetooth.DefaultAdapter
	}

	if err := ad.Enable(); err != nil {
		return nil, fmt.Errorf("ble: failed to enable BlueZ adapter: %w", err)
	}

	return ad, nil
}
