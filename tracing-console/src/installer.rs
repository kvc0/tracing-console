//! Run the public `curl … | bash` installer from inside the binary.
//!
//! Used by two paths:
//!   * `tracing-console --update` — runs at startup, exits.
//!   * The in-app version-mismatch confirm modal — runs after the
//!     TUI is torn down, exits.
//!
//! Both end the process so the user can re-launch the upgraded
//! binary from their shell.

use std::io;
use std::process::{Command, ExitStatus};

/// URL of the install script.  Kept in sync with the README's
/// quick-install snippet.
pub const INSTALLER_URL: &str =
    "https://raw.githubusercontent.com/kvc0/tracing-console/main/install.sh";

/// Run `curl -fsSL <INSTALLER_URL> | bash [-s -- <version>]` with
/// stdio inherited so the user watches the installer's progress in
/// the same terminal.  `version` is the explicit version (no leading
/// `v`) or `None` for "latest".
///
/// Rejects any `version` containing characters outside `[0-9.]` so a
/// malicious server can't smuggle shell metacharacters through the
/// pipe.  (The user trusts the server enough to connect, but
/// stripping the attack surface is cheap.)
pub fn run(version: Option<&str>) -> io::Result<ExitStatus> {
    if let Some(v) = version
        && !version_is_safe(v)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to run installer for unsafe version string: {v:?}"),
        ));
    }
    let script = installer_command(version);
    Command::new("bash").arg("-c").arg(script).status()
}

/// Async sibling of [`run`] for use from inside the TUI runtime:
/// captures stdout+stderr (merged) so the version-switch modal can
/// surface installer errors on screen rather than dumping them to
/// the alt-screen-hidden terminal.  Returns the same
/// safety-validation error on a bad version string.
pub async fn run_capturing(version: Option<&str>) -> io::Result<InstallerOutcome> {
    if let Some(v) = version
        && !version_is_safe(v)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to run installer for unsafe version string: {v:?}"),
        ));
    }
    let script = installer_command(version);
    let output = tokio::process::Command::new("bash")
        .arg("-c")
        .arg(script)
        .output()
        .await?;
    // stderr first then stdout — installers tend to print progress
    // on stderr (curl) and the success line on stdout, but on
    // failure the error is on stderr.  Merging both is the simplest
    // way to make sure the user sees whatever happened.
    let mut combined = String::from_utf8_lossy(&output.stderr).into_owned();
    if !output.stdout.is_empty() {
        if !combined.is_empty() && !combined.ends_with('\n') {
            combined.push('\n');
        }
        combined.push_str(&String::from_utf8_lossy(&output.stdout));
    }
    Ok(InstallerOutcome {
        status: output.status,
        combined_output: sanitize_for_modal(&combined),
    })
}

/// Strip ANSI escape sequences and other terminal control bytes
/// from captured installer output before it's rendered into the
/// modal.  Without this, a `\x1b[…m` colour sequence (curl loves
/// these) lands in a ratatui `Span` which forwards the literal
/// bytes to the terminal — at best it paints the rest of the
/// screen in random colours, at worst it puts the terminal into a
/// state that crashes the redraw on the next frame.  Also caps the
/// total length so a runaway installer can't fill the modal with
/// megabytes of text.
fn sanitize_for_modal(s: &str) -> String {
    const MAX_BYTES: usize = 4096;
    let mut out = String::with_capacity(s.len().min(MAX_BYTES));
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if out.len() >= MAX_BYTES {
            out.push_str("\n…(truncated)");
            break;
        }
        match c {
            // ESC: consume an entire CSI sequence (`ESC [ … <final>`)
            // where the final byte is in `@`..=`~`.  Any other ESC
            // sequence is just dropped to its terminator's vicinity
            // — coarse but safe.
            '\x1b' => {
                if chars.peek() == Some(&'[') {
                    chars.next();
                    for nc in chars.by_ref() {
                        if matches!(nc, '@'..='~') {
                            break;
                        }
                    }
                }
            }
            '\n' | '\t' => out.push(c),
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod sanitize_tests {
    use super::sanitize_for_modal;

    #[test]
    fn strips_csi_color_sequences() {
        let in_ = "\x1b[31mred\x1b[0m and \x1b[1;33mbold yellow\x1b[m";
        assert_eq!(sanitize_for_modal(in_), "red and bold yellow");
    }

    #[test]
    fn keeps_newlines_and_tabs() {
        let in_ = "line one\nline\ttwo\n";
        assert_eq!(sanitize_for_modal(in_), "line one\nline\ttwo\n");
    }

    #[test]
    fn replaces_bare_control_chars() {
        // BEL, vertical tab, form feed → spaces.
        let in_ = "a\x07b\x0bc\x0cd";
        assert_eq!(sanitize_for_modal(in_), "a b c d");
    }

    #[test]
    fn caps_runaway_output() {
        let in_ = "x".repeat(100_000);
        let out = sanitize_for_modal(&in_);
        assert!(out.len() < 5000, "got len {}", out.len());
        assert!(out.ends_with("…(truncated)"));
    }
}

/// Result of an async installer run.  `combined_output` is the
/// merged stderr+stdout (lossy UTF-8) for display in the modal.
pub struct InstallerOutcome {
    pub status: ExitStatus,
    pub combined_output: String,
}

fn installer_command(version: Option<&str>) -> String {
    // `set -o pipefail` is the critical bit: without it, a `curl |
    // bash` pipeline takes its exit status from the rightmost
    // command (bash) alone, so a curl 404 against `INSTALLER_URL`
    // makes bash read an empty script and exit 0 — and we'd
    // wrongly report "installed" with nothing on disk.  With
    // pipefail, the worst exit in the pipeline wins.  install.sh
    // itself already runs under `set -euo pipefail`, but that's
    // *inside* the spawned bash; the outer `bash -c` needs its own.
    match version {
        None => format!("set -o pipefail; curl -fsSL {INSTALLER_URL} | bash"),
        Some(v) => format!("set -o pipefail; curl -fsSL {INSTALLER_URL} | bash -s -- {v}"),
    }
}

fn version_is_safe(v: &str) -> bool {
    !v.is_empty() && v.len() < 32 && v.chars().all(|c| c.is_ascii_digit() || c == '.')
}

#[cfg(test)]
mod tests {
    use super::version_is_safe;

    #[test]
    fn rejects_shell_metacharacters() {
        assert!(!version_is_safe("0.1.0; rm -rf /"));
        assert!(!version_is_safe("0.1.0`whoami`"));
        assert!(!version_is_safe("$(echo bad)"));
        assert!(!version_is_safe(""));
        assert!(!version_is_safe("v0.1.0")); // strip the v before calling
    }

    #[test]
    fn accepts_well_formed_versions() {
        for v in ["0.1.1", "10.20.30", "0.0.0"] {
            assert!(version_is_safe(v), "{v}");
        }
    }
}
