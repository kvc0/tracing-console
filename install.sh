#!/usr/bin/env bash
# Install a released `tracing-console` binary for the host platform.
# Supported targets:
#   * linux-x86_64
#   * linux-aarch64
#   * macos-aarch64
#
# Usage:
#   install.sh              # install the latest release
#   install.sh 0.1.1        # install a specific version (with or without leading 'v')
#
# Via curl|bash:
#   curl -fsSL .../install.sh | bash                # latest
#   curl -fsSL .../install.sh | bash -s -- 0.1.1    # specific
#
# Designed for `curl ... | bash`:
#   * conservative: `set -euo pipefail`
#   * no `sudo`: Installed at ~/.local/tracing-console/<version>/ and
#     symlinked to ~/.local/bin/tracing-console
#   * no ancillary changes: If ~/.local/bin isn't on PATH, only a hint is printed

set -euo pipefail

REPO="kvc0/tracing-console"
INSTALL_ROOT="$HOME/.local/tracing-console"
BIN_DIR="$HOME/.local/bin"
BIN_LINK="$BIN_DIR/tracing-console"

die()  { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n' "$*"; }

# ── arg parsing ────────────────────────────────────────────────────
# Optional positional: explicit version to install (e.g. `0.1.1` or
# `v0.1.1`).  Empty / missing means "latest release".  We accept extra
# leading whitespace because `curl ... | bash -s -- 0.1.1` is the
# documented usage and accidental quoting is easy.
requested_version=""
if [ "$#" -gt 1 ]; then
    die "too many arguments; usage: install.sh [version]"
fi
if [ "$#" -eq 1 ] && [ -n "$1" ]; then
    requested_version="${1#v}"   # strip leading 'v' if present
    case "$requested_version" in
        [0-9]*) ;;
        *) die "version must look like '0.1.1' or 'v0.1.1' (got: '$1')" ;;
    esac
fi

command -v curl >/dev/null 2>&1 \
    || die "this installer needs curl; install it and re-run"
command -v tar  >/dev/null 2>&1 \
    || die "this installer needs tar; install it and re-run"
command -v uname >/dev/null 2>&1 \
    || die "this installer needs uname; install it and re-run"

# ── target detection ────────────────────────────────────────────────
os_raw=$(uname -s)
arch_raw=$(uname -m)
case "$os_raw" in
    Linux)  os=linux ;;
    Darwin) os=macos ;;
    *) die "unsupported OS: $os_raw (only Linux and macOS are supported)" ;;
esac
case "$arch_raw" in
    x86_64|amd64)   arch=x86_64 ;;
    aarch64|arm64)  arch=aarch64 ;;
    *) die "unsupported architecture: $arch_raw" ;;
esac
target="$os-$arch"
case "$target" in
    linux-x86_64|linux-aarch64|macos-aarch64) ;;
    *) die "no prebuilt release for $target (supported: linux-x86_64, linux-aarch64, macos-aarch64)" ;;
esac

# ── version resolution ──────────────────────────────────────────────
# If the user passed an explicit version, use it as-is (tags are
# always `v<semver>`).  Otherwise resolve "latest" via the
# /releases/latest redirect — no token, no JSON parser dependency.
# The Location header points at /releases/tag/<tag>; the tail of the
# URL is the tag.
if [ -n "$requested_version" ]; then
    version="v$requested_version"
    info "> installing requested version $version of $REPO..."
else
    info "> looking up the latest release of $REPO..."
    latest_url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
                      --proto '=https' --tlsv1.2 \
                      "https://github.com/$REPO/releases/latest") \
        || die "failed to query the latest release of $REPO"
    version=$(basename "$latest_url")
    case "$version" in
        v[0-9]*) ;;
        *) die "unexpected version string from GitHub: '$version'" ;;
    esac
    info "  found $version"
fi

# ── download + extract ──────────────────────────────────────────────
archive="tracing-console-$target.tar.gz"
url="https://github.com/$REPO/releases/download/$version/$archive"
tmpdir=$(mktemp -d 2>/dev/null || mktemp -d -t 'tracing-console-install')
trap 'rm -rf "$tmpdir"' EXIT

info "> downloading $archive..."
curl -fsSL --proto '=https' --tlsv1.2 -o "$tmpdir/$archive" "$url" \
    || die "failed to download $url
       (does that version exist?  See https://github.com/$REPO/releases)"

target_dir="$INSTALL_ROOT/$version"
mkdir -p "$target_dir"
tar -xzf "$tmpdir/$archive" -C "$target_dir"
[ -f "$target_dir/tracing-console" ] \
    || die "archive did not contain a tracing-console binary"
chmod +x "$target_dir/tracing-console"

# ── macOS unblocking ────────────────────────────────────────────────
# gatekeeper and code signatures. It's not good like using apple's
# notary service, but I do not have a developer subscription.
if [ "$os" = "macos" ]; then
    xattr -dr com.apple.quarantine "$target_dir/tracing-console" 2>/dev/null || true
    codesign --sign - --force "$target_dir/tracing-console" >/dev/null 2>&1 || true
fi

mkdir -p "$BIN_DIR"
# `-f` overwrites an existing symlink (e.g. previous install); `-n`
# avoids descending into a directory if $BIN_LINK happens to point at
# one for some reason.
ln -sfn "$target_dir/tracing-console" "$BIN_LINK"

info "> installed $version → $target_dir/tracing-console"
info "  symlinked at $BIN_LINK"

# ── PATH check ──────────────────────────────────────────────────────
case ":$PATH:" in
    *":$BIN_DIR:"*)
        info ""
        info "✓ $BIN_DIR is on your PATH — run \`tracing-console\` to start."
        ;;
    *)
        shell_name=$(basename "${SHELL:-/bin/sh}")
        case "$shell_name" in
            bash)
                # On macOS interactive login shells read ~/.bash_profile
                # by default; on Linux they read ~/.bashrc.
                if [ "$os" = "macos" ]; then
                    rc="$HOME/.bash_profile"
                else
                    rc="$HOME/.bashrc"
                fi
                snippet='export PATH="$HOME/.local/bin:$PATH"'
                ;;
            zsh)
                rc="$HOME/.zshrc"
                snippet='export PATH="$HOME/.local/bin:$PATH"'
                ;;
            fish)
                rc="$HOME/.config/fish/config.fish"
                snippet='set -gx PATH $HOME/.local/bin $PATH'
                ;;
            *)
                rc=""
                snippet='export PATH="$HOME/.local/bin:$PATH"'
                ;;
        esac
        info ""
        info "⚠ $BIN_DIR is not on your PATH."
        if [ -n "$rc" ]; then
            info "  Add it for $shell_name with:"
            info ""
            info "    echo '$snippet' >> $rc"
            info ""
            info "  Then open a new terminal (or \`source $rc\`)."
        else
            info "  Couldn't auto-detect your shell (\$SHELL=${SHELL:-unset})."
            info "  Add this to your shell's rc file:"
            info "    $snippet"
        fi
        info ""
        info "  Until then, you can run it via its full path:"
        info "    $BIN_LINK"
        ;;
esac
