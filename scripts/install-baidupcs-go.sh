#!/usr/bin/env sh
set -eu

bin_name="${BAIDUPCS_BIN:-BaiduPCS-Go}"
case "$bin_name" in
  */*) install_path="$bin_name" ;;
  *) install_path="/usr/local/bin/$bin_name" ;;
esac

if command -v "$bin_name" >/dev/null 2>&1; then
  exit 0
fi

arch="$(uname -m)"
case "$arch" in
  x86_64 | amd64) release_arch="amd64" ;;
  aarch64 | arm64) release_arch="arm64" ;;
  *)
    echo "Unsupported BaiduPCS-Go architecture: $arch" >&2
    exit 1
    ;;
esac

version="${BAIDUPCS_GO_VERSION:-v4.0.1}"
if [ "$version" = "latest" ]; then
  release_json="$(curl -fsSL https://api.github.com/repos/qjfoidnh/BaiduPCS-Go/releases/latest)"
  download_url="$(printf "%s" "$release_json" | grep -E "browser_download_url.*linux-${release_arch}\\.zip" | head -n 1 | sed -E 's/.*"([^"]+)".*/\1/')"
else
  download_url="https://github.com/qjfoidnh/BaiduPCS-Go/releases/download/${version}/BaiduPCS-Go-${version}-linux-${release_arch}.zip"
fi

if [ -z "${download_url:-}" ]; then
  echo "Could not resolve BaiduPCS-Go download URL for linux-${release_arch}." >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "Installing BaiduPCS-Go from $download_url"
curl -fL "$download_url" -o "$tmp_dir/baidupcs-go.zip"
unzip -q "$tmp_dir/baidupcs-go.zip" -d "$tmp_dir"
bin_path="$(find "$tmp_dir" -type f -name BaiduPCS-Go | head -n 1)"
if [ -z "$bin_path" ]; then
  echo "BaiduPCS-Go binary not found in release archive." >&2
  exit 1
fi

mkdir -p "$(dirname "$install_path")"
install -m 0755 "$bin_path" "$install_path"
if [ "$install_path" != "/usr/local/bin/BaiduPCS-Go" ]; then
  ln -sf "$install_path" /usr/local/bin/BaiduPCS-Go
fi

"$install_path" --version >/dev/null || true
