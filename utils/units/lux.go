// Copyright (C) 2019-2025, Lux Industries Inc All rights reserved.
// See the file LICENSE for licensing terms.

package units

// Denominations of value
const (
	NanoLux   uint64 = 1
	MicroLux  uint64 = 1000 * NanoLux
	Schmeckle uint64 = 49*MicroLux + 463*NanoLux
	MilliLux  uint64 = 1000 * MicroLux
	Lux       uint64 = 1000 * MilliLux
	KiloLux   uint64 = 1000 * Lux
	MegaLux   uint64 = 1000 * KiloLux
)
