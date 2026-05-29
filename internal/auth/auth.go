// Package auth verifies the login server's session token (a JWT) and binds the
// resulting player_id to the connection (PDR §6.1). Signatures are checked with
// the login server's public key (AUTH_JWT_PUBKEY); the allowed algorithms are
// pinned to the key's family so a token cannot downgrade to "none" or an HMAC
// alg-confusion attack.
package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"

	"dify_gateway/internal/session"
)

// ErrInvalidToken covers any token that fails verification (bad signature,
// malformed, wrong algorithm, missing player_id). ErrExpiredToken is returned
// specifically when the token is well-formed but past its exp.
var (
	ErrInvalidToken = errors.New("auth: invalid session token")
	ErrExpiredToken = errors.New("auth: expired session token")
)

// Authenticator verifies a session token and returns the bound player id.
type Authenticator interface {
	Verify(ctx context.Context, token string) (playerID string, err error)
}

// JWTAuthenticator verifies JWT session tokens with a fixed public key.
type JWTAuthenticator struct {
	pubKey       any
	validMethods []string
	parser       *jwt.Parser
}

type sessionClaims struct {
	PlayerID string `json:"player_id"`
	jwt.RegisteredClaims
}

// New parses a PEM-encoded public key (PKIX, or PKCS#1 for legacy RSA) and
// returns an authenticator that accepts only the algorithms matching the key.
func New(pemPublicKey string) (*JWTAuthenticator, error) {
	pubKey, err := parsePublicKey(pemPublicKey)
	if err != nil {
		return nil, err
	}
	methods, err := methodsForKey(pubKey)
	if err != nil {
		return nil, err
	}
	return &JWTAuthenticator{
		pubKey:       pubKey,
		validMethods: methods,
		parser: jwt.NewParser(
			jwt.WithValidMethods(methods),
			jwt.WithExpirationRequired(),
		),
	}, nil
}

// Verify checks the token's signature and expiry and returns its player_id.
// Failures are mapped to ErrExpiredToken or ErrInvalidToken.
func (a *JWTAuthenticator) Verify(_ context.Context, token string) (string, error) {
	claims := &sessionClaims{}
	parsed, err := a.parser.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
		return a.pubKey, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return "", ErrExpiredToken
		}
		return "", fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !parsed.Valid {
		return "", ErrInvalidToken
	}
	if claims.PlayerID == "" {
		return "", fmt.Errorf("%w: missing player_id claim", ErrInvalidToken)
	}
	return claims.PlayerID, nil
}

// BindSession runs the first-frame auth flow: verify the token, and on success
// bind the player to the session. It returns the (ok, reason) pair the access
// layer relays as AuthResult. The reason is intentionally generic so token
// internals are never leaked to the client (PDR §6, M5-T3).
func BindSession(ctx context.Context, a Authenticator, sess *session.Session, token string) (ok bool, reason string) {
	if token == "" {
		return false, "missing session token"
	}
	playerID, err := a.Verify(ctx, token)
	if err != nil {
		switch {
		case errors.Is(err, ErrExpiredToken):
			return false, "session token expired"
		default:
			return false, "invalid session token"
		}
	}
	sess.Bind(playerID)
	return true, ""
}

func parsePublicKey(pemPublicKey string) (any, error) {
	block, _ := pem.Decode([]byte(pemPublicKey))
	if block == nil {
		return nil, fmt.Errorf("auth: AUTH_JWT_PUBKEY is not valid PEM")
	}
	if pub, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		return pub, nil
	}
	// Legacy PKCS#1 RSA public key ("RSA PUBLIC KEY").
	if rsaPub, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return rsaPub, nil
	}
	return nil, fmt.Errorf("auth: AUTH_JWT_PUBKEY is not a supported public key (PKIX or PKCS#1 RSA)")
}

func methodsForKey(pubKey any) ([]string, error) {
	switch pubKey.(type) {
	case *rsa.PublicKey:
		return []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512"}, nil
	case *ecdsa.PublicKey:
		return []string{"ES256", "ES384", "ES512"}, nil
	case ed25519.PublicKey:
		return []string{"EdDSA"}, nil
	default:
		return nil, fmt.Errorf("auth: unsupported public key type %T", pubKey)
	}
}
