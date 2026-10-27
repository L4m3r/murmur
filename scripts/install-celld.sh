#!/bin/sh
set -eu

install_root="${CELLD_INSTALL_ROOT:-${HOME:-}/.local}"
release_base="${CELLD_RELEASES_URL:-https://github.com/denoland/celld/releases}"

if [ -z "$install_root" ] || [ "$install_root" = "/.local" ]; then
  echo "celld: set CELLD_INSTALL_ROOT because no home directory was detected" >&2
  exit 1
fi

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64|Linux-amd64) target="x86_64-unknown-linux-gnu" ;;
  Linux-aarch64|Linux-arm64) target="aarch64-unknown-linux-gnu" ;;
  Darwin-arm64|Darwin-aarch64) target="aarch64-apple-darwin" ;;
  *)
    echo "celld: no prebuilt release exists for $(uname -s) $(uname -m) yet" >&2
    exit 1
    ;;
esac

if [ -n "${CELLD_VERSION:-}" ]; then
  version="$CELLD_VERSION"
else
  # GitHub redirects latest/download/<asset> to the tagged asset URL; the
  # first Location header carries the tag (the final hop is a storage URL
  # that does not).
  if ! redirect="$(curl --proto '=https' --tlsv1.2 -fsSI -o /dev/null \
    -w '%{redirect_url}' \
    "$release_base/latest/download/celld-$target.gz")"; then
    echo "celld: could not resolve the latest release from $release_base" >&2
    echo "celld: check your network, or pin one with CELLD_VERSION=vX.Y.Z" >&2
    exit 1
  fi
  version="${redirect%/celld-*}"
  version="${version##*/}"
fi

# A version tag, never a path.
case "$version" in
  v[0-9]*) ;;
  *) echo "celld: release version must be a tag such as v0.0.1" >&2; exit 1 ;;
esac
case "$version" in
  *[!0-9A-Za-z.-]*)
    echo "celld: release version contains invalid characters" >&2
    exit 1
    ;;
esac

bin_dir="$install_root/bin"
release_dir="$install_root/lib/celld/releases/$version"
release_url="$release_base/download/$version"
mkdir -p "$bin_dir" "$install_root/lib/celld/releases"

if [ ! -x "$release_dir/celld" ]; then
  staging="$(mktemp -d "$install_root/lib/celld/.install.XXXXXX")"
  trap '[ -z "$staging" ] || rm -rf "$staging"' EXIT HUP INT TERM
  echo "Downloading celld $version for $target"
  if ! curl --proto '=https' --tlsv1.2 -fsSL \
    "$release_url/celld-$target.gz" -o "$staging/celld.gz"; then
    echo "celld: failed to download $release_url/celld-$target.gz" >&2
    echo "celld: does release $version exist and is it public?" >&2
    exit 1
  fi
  # gzip's CRC rejects a corrupt or truncated download; provenance is the
  # release attestation (gh attestation verify <asset> --repo denoland/celld).
  gzip -dc "$staging/celld.gz" > "$staging/celld"
  chmod 755 "$staging/celld"
  rm "$staging/celld.gz"
  # The release directory appears only after the download decompresses,
  # atomically.
  mv "$staging" "$release_dir"
  staging=""
fi

# The entrypoint is one symlink, swapped atomically; rerunning with a
# previous CELLD_VERSION is a rollback.
swap="$bin_dir/.celld.$$"
ln -s "../lib/celld/releases/$version/celld" "$swap"
mv -f "$swap" "$bin_dir/celld"

echo
echo "Installed celld $version at $bin_dir/celld"
echo "'celld deploy' needs esbuild (https://esbuild.github.io) on PATH."
echo
echo "Next:"
echo "  $bin_dir/celld --help"

case ":${PATH:-}:" in
  *":$install_root/bin:"*) ;;
  *)
    echo
    echo "Add $install_root/bin to PATH:"
    echo "  export PATH=\"$install_root/bin:\$PATH\""
    ;;
esac
