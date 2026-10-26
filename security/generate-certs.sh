#!/usr/bin/env bash
set -euo pipefail

mkdir -p security/certs
cd security/certs

PASS="featureforge-secret"

# 1. Generate Root CA
openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 -passin pass:$PASS -passout pass:$PASS \
  -subj "/CN=FeatureForge-Root-CA/O=FeatureForge"

# Helper function to issue PKCS12 keystores signed by our CA
issue_cert() {
  local NAME=$1
  keytool -genkeypair -alias "$NAME" -keyalg RSA -keysize 2048 -storetype PKCS12 \
    -keystore "$NAME.keystore.p12" -storepass "$PASS" -keypass "$PASS" \
    -dname "CN=$NAME,O=FeatureForge" \
    -ext "SAN=dns:$NAME,dns:localhost,ip:127.0.0.1"

  keytool -certreq -alias "$NAME" -keystore "$NAME.keystore.p12" -storepass "$PASS" -file "$NAME.csr"

  openssl x509 -req -CA ca.crt -CAkey ca.key -in "$NAME.csr" -out "$NAME.crt" \
    -days 365 -CAcreateserial -passin pass:$PASS \
    -extfile <(printf "subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1" "$NAME")

  keytool -importcert -alias ca -file ca.crt -keystore "$NAME.keystore.p12" -storepass "$PASS" -noprompt
  keytool -importcert -alias "$NAME" -file "$NAME.crt" -keystore "$NAME.keystore.p12" -storepass "$PASS" -noprompt
}

for SERVICE in kafka redis ingest stream scoring; do
  issue_cert "$SERVICE"
done

# Create universal truststore
keytool -importcert -alias ca -file ca.crt -storetype PKCS12 -keystore truststore.p12 -storepass "$PASS" -noprompt

# Generate a self-signed "wrong authority" keystore for negative security tests
keytool -genkeypair -alias bad-client -keyalg RSA -keysize 2048 -storetype PKCS12 \
  -keystore bad-client.keystore.p12 -storepass "$PASS" -keypass "$PASS" \
  -dname "CN=bad-client,O=Untrusted"

echo "PKCS12 mTLS certificates generated successfully."