// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package keychain

//go:generate go run go.uber.org/mock/mockgen -package=${GOPACKAGE}mock -destination=${GOPACKAGE}mock/ledger.go -mock_names=Ledger=Ledger . Ledger
