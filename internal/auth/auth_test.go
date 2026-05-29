package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"dify_gateway/internal/session"
)

func pubKeyPEM(t *testing.T, pub any) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func newRSAAuth(t *testing.T) (*JWTAuthenticator, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	a, err := New(pubKeyPEM(t, &key.PublicKey))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return a, key
}

func signRS256(t *testing.T, key *rsa.PrivateKey, claims *sessionClaims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

func validClaims(playerID string) *sessionClaims {
	return &sessionClaims{
		PlayerID:         playerID,
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	}
}

func TestVerifyValidToken(t *testing.T) {
	a, key := newRSAAuth(t)
	token := signRS256(t, key, validClaims("player-42"))

	got, err := a.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if got != "player-42" {
		t.Fatalf("playerID = %q, want player-42", got)
	}
}

func TestVerifyExpiredToken(t *testing.T) {
	a, key := newRSAAuth(t)
	token := signRS256(t, key, &sessionClaims{
		PlayerID:         "player-1",
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour))},
	})

	_, err := a.Verify(context.Background(), token)
	if !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("Verify() error = %v, want ErrExpiredToken", err)
	}
}

func TestVerifyTamperedToken(t *testing.T) {
	a, key := newRSAAuth(t)
	token := signRS256(t, key, validClaims("player-1"))
	tampered := token[:len(token)-3] + "AAA" // corrupt the signature segment

	_, err := a.Verify(context.Background(), tampered)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
	}
}

func TestVerifyWrongSigningKey(t *testing.T) {
	a, _ := newRSAAuth(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	token := signRS256(t, otherKey, validClaims("player-1"))

	_, err = a.Verify(context.Background(), token)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
	}
}

func TestVerifyRejectsNoneAlg(t *testing.T) {
	a, _ := newRSAAuth(t)
	token, err := jwt.NewWithClaims(jwt.SigningMethodNone, validClaims("player-1")).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none token: %v", err)
	}

	_, err = a.Verify(context.Background(), token)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
	}
}

func TestVerifyRejectsHMACAlgConfusion(t *testing.T) {
	a, key := newRSAAuth(t)
	// Classic attack: sign with HS256 using the public key bytes as the secret.
	secret := []byte(pubKeyPEM(t, &key.PublicKey))
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims("player-1")).SignedString(secret)
	if err != nil {
		t.Fatalf("sign hmac token: %v", err)
	}

	_, err = a.Verify(context.Background(), token)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken (alg must be pinned)", err)
	}
}

func TestVerifyRequiresExpiry(t *testing.T) {
	a, key := newRSAAuth(t)
	token := signRS256(t, key, &sessionClaims{PlayerID: "player-1"}) // no exp

	_, err := a.Verify(context.Background(), token)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken (exp required)", err)
	}
}

func TestVerifyRejectsMissingPlayerID(t *testing.T) {
	a, key := newRSAAuth(t)
	token := signRS256(t, key, &sessionClaims{
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	})

	_, err := a.Verify(context.Background(), token)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken (missing player_id)", err)
	}
}

func TestNewSupportsECDSA(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ecdsa key: %v", err)
	}
	a, err := New(pubKeyPEM(t, &key.PublicKey))
	if err != nil {
		t.Fatalf("New(ECDSA) error = %v", err)
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodES256, validClaims("player-9")).SignedString(key)
	if err != nil {
		t.Fatalf("sign es256: %v", err)
	}
	got, err := a.Verify(context.Background(), token)
	if err != nil || got != "player-9" {
		t.Fatalf("Verify(ES256) = %q, %v", got, err)
	}
}

func TestNewRejectsBadPEM(t *testing.T) {
	if _, err := New("not a pem"); err == nil {
		t.Fatal("New(bad pem) error = nil, want error")
	}
}

func TestBindSession(t *testing.T) {
	a, key := newRSAAuth(t)

	t.Run("valid binds player", func(t *testing.T) {
		sess := session.New("conn-1", "127.0.0.1:1")
		token := signRS256(t, key, validClaims("player-7"))
		ok, reason := BindSession(context.Background(), a, sess, token)
		if !ok || reason != "" {
			t.Fatalf("BindSession() = %v, %q", ok, reason)
		}
		if !sess.Authenticated() || sess.PlayerID() != "player-7" {
			t.Fatalf("session not bound: authed=%v player=%q", sess.Authenticated(), sess.PlayerID())
		}
	})

	t.Run("empty token", func(t *testing.T) {
		sess := session.New("conn-1", "127.0.0.1:1")
		ok, reason := BindSession(context.Background(), a, sess, "")
		if ok || reason != "missing session token" {
			t.Fatalf("BindSession() = %v, %q", ok, reason)
		}
		if sess.Authenticated() {
			t.Fatal("session bound on empty token")
		}
	})

	t.Run("expired token", func(t *testing.T) {
		sess := session.New("conn-1", "127.0.0.1:1")
		token := signRS256(t, key, &sessionClaims{
			PlayerID:         "player-1",
			RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))},
		})
		ok, reason := BindSession(context.Background(), a, sess, token)
		if ok || reason != "session token expired" {
			t.Fatalf("BindSession() = %v, %q", ok, reason)
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		sess := session.New("conn-1", "127.0.0.1:1")
		ok, reason := BindSession(context.Background(), a, sess, "garbage.token.value")
		if ok || reason != "invalid session token" {
			t.Fatalf("BindSession() = %v, %q", ok, reason)
		}
	})
}
