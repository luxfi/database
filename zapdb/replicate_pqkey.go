// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

// Post-quantum encryption keys for replication, out of the box.
//
// Backups are age-encrypted client-side BEFORE they reach S3, so the hanzo-s3
// (MinIO-based) server never sees plaintext — its own server-side encryption is
// classical and trusts the server, which is exactly what we don't. The default
// here is the post-quantum hybrid (ML-KEM-768 + X25519): a single key file, with
// the recipient derived from the identity, gives PQ encrypt+decrypt with no
// out-of-band key wrangling. PQ costs ~1.6 KB and ~0.1 ms per object — free.

import (
	"fmt"
	"os"
	"strings"

	"github.com/luxfi/age"
	log "github.com/luxfi/log"
)

// resolveReplicationIdentity returns the age identity used to DECRYPT backups:
// REPLICATE_AGE_IDENTITY if set, otherwise generated-or-loaded from
// REPLICATE_AGE_KEY_FILE (the out-of-box PQ path). nil → plaintext. Shared by
// the replicator and the pre-open physical hydrate so both decrypt with the same
// key — a mismatch here is what made physical restore fail without an identity.
func resolveReplicationIdentity() age.Identity {
	s := os.Getenv("REPLICATE_AGE_IDENTITY")
	if s == "" {
		if kf := os.Getenv("REPLICATE_AGE_KEY_FILE"); kf != "" {
			k, err := loadOrCreatePQKey(kf)
			if err != nil {
				log.Warn(fmt.Sprintf("[zapdb] replicate: PQ key file %s: %v", kf, err))
				return nil
			}
			s = k
		}
	}
	if s == "" {
		return nil
	}
	ids, err := age.ParseIdentities(strings.NewReader(s))
	if err != nil || len(ids) == 0 {
		log.Warn(fmt.Sprintf("[zapdb] replicate: parse age identity: %v", err))
		return nil
	}
	return ids[0]
}

// recipientFor returns the public recipient that matches an age identity, so one
// secret configures both encrypt and decrypt. nil for identity types without a
// derivable recipient (e.g. scrypt/passphrase).
func recipientFor(id age.Identity) age.Recipient {
	switch i := id.(type) {
	case *age.HybridIdentity:
		return i.Recipient()
	case *age.XWingIdentity:
		return i.Recipient()
	case *age.X25519Identity:
		return i.Recipient()
	default:
		return nil
	}
}

// loadOrCreatePQKey returns the age identity string at path, generating a fresh
// post-quantum (ML-KEM-768 + X25519) key there on first use. The identity is
// written 0600 and the public recipient to "<path>.pub" for sharing. Keep this
// file OUTSIDE the replicated DB dir and back it up: it is the only thing that
// can decrypt the backups (lose it and the snapshots are unrecoverable).
func loadOrCreatePQKey(path string) (string, error) {
	if b, err := os.ReadFile(path); err == nil {
		s := strings.TrimSpace(string(b))
		if s == "" {
			return "", fmt.Errorf("key file is empty")
		}
		return s, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	id, err := age.GeneratePQIdentity(age.PQKemHPKEMLKEM768X25519)
	if err != nil {
		return "", fmt.Errorf("generate PQ identity: %w", err)
	}
	secret := fmt.Sprint(id) // AGE-SECRET-KEY-PQ-1...
	if err := os.WriteFile(path, []byte(secret+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write key file: %w", err)
	}
	if r := recipientFor(id); r != nil {
		rs := fmt.Sprint(r) // age1pq1...
		_ = os.WriteFile(path+".pub", []byte(rs+"\n"), 0o644)
		log.Info(fmt.Sprintf("[zapdb] replicate: generated post-quantum key %s (recipient %s) — BACK THIS UP", path, rs))
	}
	return secret, nil
}
