#!/bin/sh
# Adds the egress proxy's CA to the sandbox's system trust store, writes a combined bundle for
# clients that read SSL_CERT_FILE and similar, and imports it into Java's store when there is one.
# Usage: install_ca.sh CERT_FILE BUNDLE_FILE
set -eu

cert=$1
bundle=$2

install_ca() {
    if command -v update-ca-certificates >/dev/null 2>&1 && [ -d /usr/local/share/ca-certificates ]; then
        install -m 0644 "$cert" /usr/local/share/ca-certificates/egress-proxy.crt
        update-ca-certificates
        system_bundle=/etc/ssl/certs/ca-certificates.crt
    elif command -v update-ca-trust >/dev/null 2>&1 && [ -d /etc/pki/ca-trust/source/anchors ]; then
        install -m 0644 "$cert" /etc/pki/ca-trust/source/anchors/egress-proxy.crt
        update-ca-trust extract
        system_bundle=/etc/pki/tls/certs/ca-bundle.crt
    else
        echo "No supported system CA trust store found" >&2
        return 1
    fi
    cat "$system_bundle" "$cert" > "$bundle"
    chmod 0644 "$bundle"
    if command -v keytool >/dev/null 2>&1; then
        keytool -delete -alias egress-proxy-ca -cacerts -storepass changeit >/dev/null 2>&1 || true
        keytool -importcert -cacerts -noprompt -storepass changeit -alias egress-proxy-ca -file "$cert"
    fi
}

if [ "$(id -u)" -eq 0 ] || [ "${EGRESS_PROXY_CA_ATTEMPT:-}" = unprivileged ]; then
    install_ca
    exit 0
fi
# Each attempt runs in a fresh shell: set -e doesn't apply to commands run as an if condition.
if EGRESS_PROXY_CA_ATTEMPT=unprivileged sh "$0" "$cert" "$bundle" 2>/dev/null; then
    exit 0
fi
if command -v sudo >/dev/null 2>&1 && sudo -n sh "$0" "$cert" "$bundle"; then
    exit 0
fi
echo "Egress proxy CA trust installation requires root or non-interactive sudo" >&2
exit 1
