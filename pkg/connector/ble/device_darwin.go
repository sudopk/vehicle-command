package ble

import (
	"fmt"

	"tinygo.org/x/bluetooth"
)

func IsAdapterError(_ error) bool {
	return false
}

func AdapterErrorHelpMessage(err error) string {
	return err.Error()
}

func newAdapter(id *string) (*bluetooth.Adapter, error) {
	ad := bluetooth.DefaultAdapter
	if err := ad.Enable(); err != nil {
		return nil, fmt.Errorf("ble: failed to enable adapter: %w", err)
	}
	return ad, nil
}
