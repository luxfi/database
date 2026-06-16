# ZAP ZapDB backup schema v1
#
# Canonical, language-agnostic wire format for a ZapDB (Badger fork) backup
# stream: a length-framed sequence of KV records. This is the SINGLE SOURCE OF
# TRUTH that replaces the hand-rolled sequential encoder in
# github.com/luxfi/zapdb/pb/marshal_zap.go (which is "zap" in name only — a
# sequential [u32 len][bytes]… layout with no fixed offsets and no zero-copy
# reads).
#
# A KV here is a true ZAP struct: a fixed 48-byte header of scalars and
# (offset:u32, length:u32) pointer pairs, with the variable-length key / value /
# meta payloads living in the heap area that follows. A reader resolves any field
# in O(1) from its compile-time offset — no scanning, no per-field allocation.
#
# Backup framing (unchanged from the current stream so a bucket stays mixed-
# readable during migration): each record is `len(KV):u64-LE || KV bytes`,
# concatenated; EOF terminates. The fixed-header/heap/pointer LAYOUT rules this
# schema is written against live in github.com/zap-proto/zap-spec (SPEC.md); this
# file is ZapDB's own schema, colocated with its reference codec (zapkv.go).
#
# Wire stability: this is the v1 ZapDB wire format. Per the project's
# "no backwards compatibility, only forwards perfection" rule, a shape change
# means a new struct name, not a silent offset shift.

package zapdb

# A single key/value version entry. Fixed header = 48 bytes; heap follows.
struct KV {
    Version    u64    @0     # Badger commit timestamp (monotonic version)
    ExpiresAt  u64    @8     # unix-seconds TTL, 0 = never
    Key        bytes  @16    # (offset:u32, length:u32) into heap
    Value      bytes  @24    # (offset:u32, length:u32) into heap
    UserMeta   bytes  @32    # (offset:u32, length:u32) into heap — usually 1 byte
    Meta       bytes  @40    # (offset:u32, length:u32) into heap
}

# A batch of KV records (the unit Badger's Stream framework emits per key range).
struct KVList {
    Kv  list<KV>  @0
}
