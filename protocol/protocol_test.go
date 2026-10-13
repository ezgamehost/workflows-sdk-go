package protocol

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestSignatureVectors(t *testing.T) {
	var f struct {
		Sign []struct {
			Secrets []string
			T       int64
			Body    string
			Header  string
		}
		Verify []struct {
			Secrets []string
			Header  string
			Body    string
			Now     int64
			Result  string
		}
	}
	b, err := os.ReadFile("../testdata/vectors/signatures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &f); err != nil || len(f.Sign) == 0 || len(f.Verify) == 0 {
		t.Fatalf("vectors: %v", err)
	}
	keys := func(secrets []string) [][]byte {
		var out [][]byte
		for _, s := range secrets {
			k, err := DecodeSecret(s)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, k)
		}
		return out
	}
	for _, c := range f.Sign {
		if got := Sign(keys(c.Secrets), time.Unix(c.T, 0), []byte(c.Body)); got != c.Header {
			t.Errorf("Sign = %s, want %s", got, c.Header)
		}
	}
	for i, c := range f.Verify {
		err := Verify(c.Header, keys(c.Secrets), []byte(c.Body), time.Unix(c.Now, 0))
		got := "ok"
		var se *SignatureError
		if errors.As(err, &se) {
			got = se.Reason
		}
		if got != c.Result {
			t.Errorf("verify[%d]: %s, want %s", i, got, c.Result)
		}
	}
	if v := Versions(); len(v) != 3 || v[0] != "1.0" || v[1] != "1.1" || v[2] != "1.2" || Version != "1.2" {
		t.Fatalf("versions %v %s", v, Version)
	}
}
