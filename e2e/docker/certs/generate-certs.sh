#!/usr/bin/env bash
# Generate test SSL certificates for MySQL E2E testing.
# Run from the directory containing this script.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Clean previous
rm -f *.pem *.srl

# --- CA ---
openssl genrsa 2048 > ca-key.pem 2>/dev/null
openssl req -new -x509 -nodes -days 3650 -key ca-key.pem \
  -subj "/CN=MES Test CA" -out ca.pem 2>/dev/null

# --- Server cert (CN=localhost, SAN=127.0.0.1,localhost) ---
openssl genrsa 2048 > server-key.pem 2>/dev/null
openssl req -new -key server-key.pem -subj "/CN=localhost" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
  -out server-req.pem 2>/dev/null
openssl x509 -req -in server-req.pem -days 3650 \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -copy_extensions copyall \
  -out server-cert.pem 2>/dev/null
rm -f server-req.pem

# --- Client cert ---
openssl genrsa 2048 > client-key.pem 2>/dev/null
openssl req -new -key client-key.pem -subj "/CN=MES Test Client" \
  -out client-req.pem 2>/dev/null
openssl x509 -req -in client-req.pem -days 3650 \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out client-cert.pem 2>/dev/null
rm -f client-req.pem

# --- Wrong CA (not in the chain, for negative tests) ---
openssl genrsa 2048 > wrong-ca-key.pem 2>/dev/null
openssl req -new -x509 -nodes -days 3650 -key wrong-ca-key.pem \
  -subj "/CN=Wrong CA" -out wrong-ca.pem 2>/dev/null

rm -f *.srl

echo "Certificates generated in $DIR:"
ls -1 *.pem
