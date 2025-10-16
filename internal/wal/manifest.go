package wal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// WAL format enumeration
type WALFormat string

const (
	WALFormatLegacy WALFormat = "legacy"
	WALFormatUWAL1  WALFormat = "uwal1"
	WALFormatMixed  WALFormat = "mixed"
)

// EnforceMode governs how mismatches are handled
type EnforceMode string

const (
	EnforceWarn   EnforceMode = "warn"
	EnforceStrict EnforceMode = "strict"
)

// Manifest captures WAL directory metadata and policy.
// schemaVersion guards structural evolves of this manifest file itself.
type Manifest struct {
	SchemaVersion int         `json:"schemaVersion"` // bump on manifest structure changes
	Format        WALFormat   `json:"format"`        // legacy | uwal1 | mixed
	UWALVersion   int         `json:"uwalVersion"`   // 1 for UWAL1, 0 for legacy
	Enforce       EnforceMode `json:"enforce"`       // warn | strict
	CreatedAt     time.Time   `json:"createdAt"`
	LastUpdatedAt time.Time   `json:"lastUpdatedAt"`
	WriterVersion string      `json:"writerVersion"`
	// Optional features, reserved for future
	Features struct {
		Encryption  string `json:"encryption,omitempty"`  // none | aes256-gcm
		Compression string `json:"compression,omitempty"` // none | zstd
		BlockSize   int    `json:"blockSize,omitempty"`
	} `json:"features"`
	Compat struct {
		MixedOK      bool `json:"mixedOk,omitempty"`
		LegacyAccept bool `json:"legacyAccept,omitempty"`
	} `json:"compat"`
}

func (m *Manifest) IsUWAL1() bool { return m.Format == WALFormatUWAL1 && m.UWALVersion == 1 }

const manifestFileName = "WAL.MANIFEST"

// LoadManifest loads a WAL manifest from dir.
func LoadManifest(dir string) (*Manifest, []byte, error) {
	path := filepath.Join(dir, manifestFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var mf Manifest
	if err := json.Unmarshal(b, &mf); err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(b)
	return &mf, sum[:], nil
}

// SaveManifestAtomic writes the manifest atomically with fsyncs.
func SaveManifestAtomic(dir string, m *Manifest) ([]byte, error) {
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	m.LastUpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	tmp := filepath.Join(dir, manifestFileName+".tmp")
	target := filepath.Join(dir, manifestFileName)
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return nil, err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, err
	}
	_ = f.Close()
	if err := os.Rename(tmp, target); err != nil {
		return nil, err
	}
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return nil, err
	}
	_ = d.Close()
	sum := sha256.Sum256(data)
	return sum[:], nil
}

// DetectFormat scans a small prefix of entries to infer WAL format.
// It returns legacy, uwal1, or mixed.
func DetectFormat(dir string, maxSegments int, maxEntries int) (WALFormat, error) {
	// Reuse walForge helpers by ad-hoc reader; minimal duplication for now.
	// We open up to maxSegments earliest segments and scan up to maxEntries entries total.
	segments, err := (&walForge{}).segmentsInDir(dir)
	if err != nil {
		return "", err
	}
	if len(segments) == 0 {
		return WALFormatUWAL1, nil
	} // empty dir → future is UWAL1
	segLimit := len(segments)
	if maxSegments > 0 && maxSegments < segLimit {
		segLimit = maxSegments
	}
	uwalCount := 0
	legacyCount := 0
	entriesScanned := 0
	for i := 0; i < segLimit; i++ {
		f, err := os.Open(segments[i])
		if err != nil {
			return "", err
		}
		// Read crc(4) + size(4) + payload(size)
		for entriesScanned < maxEntries || maxEntries == 0 {
			hdr := make([]byte, 8)
			if _, err := io.ReadFull(f, hdr); err != nil {
				break
			}
			sz := int(uint32(hdr[4]) | uint32(hdr[5])<<8 | uint32(hdr[6])<<16 | uint32(hdr[7])<<24)
			if sz <= 0 {
				break
			}
			if sz > 16<<20 { // sanity cap
				break
			}
			payload := make([]byte, sz)
			if _, err := io.ReadFull(f, payload); err != nil {
				break
			}
			if isUWAL(payload) {
				uwalCount++
			} else {
				legacyCount++
			}
			entriesScanned++
			if maxEntries > 0 && entriesScanned >= maxEntries {
				break
			}
		}
		_ = f.Close()
		if maxEntries > 0 && entriesScanned >= maxEntries {
			break
		}
	}
	if uwalCount > 0 && legacyCount == 0 {
		return WALFormatUWAL1, nil
	}
	if uwalCount == 0 && legacyCount > 0 {
		return WALFormatLegacy, nil
	}
	if uwalCount == 0 && legacyCount == 0 {
		return WALFormatUWAL1, nil
	} // empty segments → treat as uwal1
	return WALFormatMixed, nil
}

// VerifyAgainst enforces manifest policy versus detected format.
func (m *Manifest) VerifyAgainst(detected WALFormat) error {
	if m.Enforce == EnforceWarn {
		return nil
	}
	if m.Enforce != EnforceStrict {
		return errors.New("invalid enforce mode in manifest")
	}
	if m.IsUWAL1() {
		switch detected {
		case WALFormatUWAL1:
			return nil
		case WALFormatMixed:
			if m.Compat.MixedOK {
				return nil
			}
			return fmt.Errorf("WAL format mismatch: manifest=UWAL1(strict) detected=mixed")
		default:
			return fmt.Errorf("WAL format mismatch: manifest=UWAL1(strict) detected=%s", detected)
		}
	}
	// legacy manifest in strict mode: disallow UWAL appends in write path; reader may still proceed.
	return nil
}

// Hex encodes a hash
func hexHash(b []byte) string { return hex.EncodeToString(b) }
