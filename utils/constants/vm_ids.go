// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package constants

import "github.com/luxfi/ids"

const (
	PlatformVMName  = "platformvm"
	XVMName         = "xvm"
	EVMName         = "evm"
	XSVMName        = "xsvm"
	QVMName         = "qvm"
)

var (
	PlatformVMID  = ids.ID{'p', 'l', 'a', 't', 'f', 'o', 'r', 'm', 'v', 'm'}
	XVMID         = ids.ID{'a', 'v', 'm'}
	EVMID         = ids.ID{'e', 'v', 'm'}
	XSVMID        = ids.ID{'x', 's', 'v', 'm'}
	QVMID         = ids.ID{'q', 'v', 'm'}
)

// VMName returns the name of the VM with the provided ID. If a human readable
// name isn't known, then the formatted ID is returned.
func VMName(vmID ids.ID) string {
	switch vmID {
	case PlatformVMID:
		return PlatformVMName
	case XVMID:
		return XVMName
	case EVMID:
		return EVMName
	case XSVMID:
		return XSVMName
	case QVMID:
		return QVMName
	default:
		return vmID.String()
	}
}
