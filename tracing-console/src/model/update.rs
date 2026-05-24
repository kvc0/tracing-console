//! `Update` enum — every state change that can move the model —
//! plus the `Effect` enum the reducer returns to signal side
//! effects to the runtime.

use serde::{Deserialize, Serialize};
use tracing_console_host::{WireLevelFilter, WireSpan};
// `WireServerInfo` is referenced via fully-qualified path in the
// `ServerInfoReceived` variant so the `use` block stays narrow.

use super::graph::AggMode;

/// Every state-change message that can move the model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Update {
    SpanReceived(WireSpan),
    SelectUp,
    SelectDown,
    /// Expand the highlighted row's direct children (one level).
    ExpandSelected,
    /// Expand every descendant of the highlighted row (recursive).
    ExpandAllSelected,
    /// Collapse the highlighted row, or if already collapsed, jump
    /// up to and collapse the parent.
    CollapseSelected,
    /// Tab: swap focus between Stacks and Details panes.
    SwitchFocus,
    /// In Details focus: toggle the highlighted metadata key in/out
    /// of `split_keys`.
    ToggleSplitSelected,
    /// Server pushed its handshake `ServerInfo` (the very first
    /// message on every `StartStream`).  Carries the server crate's
    /// version so the UI can flag a client/server mismatch.
    ServerInfoReceived(tracing_console_host::WireServerInfo),
    /// Server pushed the current cache-recording level — display
    /// state is updated to reflect this (and only this).
    CacheLevelReceived(WireLevelFilter),
    /// User pressed a Shift+letter shortcut to request a new cache
    /// level.  The model returns `Effect::RequestSetLevel`, which
    /// the runtime turns into an outgoing `SetCacheLevel` RPC.
    /// `cache_level` does *not* change here — it only flips when the
    /// server pushes its `CacheLevel` reply back.
    RequestCacheLevel(WireLevelFilter),
    /// Server pushed the current cache-recording chance percentage.
    CacheChanceReceived(f64),
    /// User pressed `C` (with the level switcher visible) to begin
    /// editing the chance percentage.  Initialises `chance_input` to
    /// an empty buffer.
    BeginChanceInput,
    /// User typed a digit / `.` while editing the chance.  Anything
    /// else is silently ignored.
    ChanceInputChar(char),
    /// User pressed Backspace while editing the chance.
    ChanceInputBackspace,
    /// User pressed `Esc` — cancel chance input without commit.
    ChanceInputCancel,
    /// User pressed `Enter` while editing the chance.  If the buffer
    /// parses as an `f64` in `[0.0, 100.0]`, the model emits
    /// `Effect::RequestSetChance(value)`; otherwise it silently
    /// reverts (buffer is cleared, `cache_chance` stays unchanged).
    ChanceInputCommit,
    // ── Graph view ──────────────────────────────────────────────
    /// `g`: enter graph mode locked onto the currently-highlighted
    /// row, or leave graph mode if already in it.  No-op if Table
    /// mode is active and nothing is highlighted.
    ToggleGraph,
    /// Replace graph aggregation outright.  Issued by the
    /// aggregation-input modal commit and by the test suite.
    SetGraphAgg(AggMode),
    /// `t`: flip metric between `Total` and `SelfTime`.  Wipes the
    /// store because the bin scalars are metric-specific.
    ToggleGraphMetric,
    /// `a`: open the aggregation-expression modal.  The buffer
    /// accepts a freeform expression — `a`/`avg`, `min`, `max`,
    /// or `pX[.XX]` — that's parsed at commit time via
    /// [`super::graph::parse_agg_input`].
    BeginGraphAggInput,
    GraphAggInputChar(char),
    GraphAggInputBackspace,
    GraphAggInputCancel,
    GraphAggInputCommit,
    /// `w`: open the window-size-input modal.
    BeginGraphWindowInput,
    GraphWindowInputChar(char),
    GraphWindowInputBackspace,
    GraphWindowInputCancel,
    GraphWindowInputCommit,
    /// `l`: open the lookback-input modal.  Buffer accepts digits,
    /// one `.`, and an optional trailing `s`/`m` unit suffix.
    BeginGraphLookbackInput,
    GraphLookbackInputChar(char),
    GraphLookbackInputBackspace,
    GraphLookbackInputCancel,
    GraphLookbackInputCommit,
    /// `u`: cycle the chart's X-axis label format
    /// (`Delta → Unix → Local → Delta`).
    ToggleGraphTimeLabels,
    /// `e` from the stacks table: open explore mode on the
    /// currently-selected bucket.  Sends a `RequestSetLevel(Off)`
    /// effect so the cache stops growing while the user reads.
    EnterExplore,
    /// `e` / `Esc` from explore mode: return to the previous
    /// view and restore the cache level captured on entry.
    ExitExplore,
    ExploreSelectUp,
    ExploreSelectDown,
    /// `Left` / `Right` in explore: cycle the leading sort column
    /// (time → latency → field cols → time).
    ExploreSortLeft,
    ExploreSortRight,
    /// `i` in explore: invert the active sort direction
    /// (ascending ↔ descending) without changing column.
    ExploreInvertSort,
    /// `/`: open the search modal.  Live-filters the row list as
    /// the user types.
    BeginExploreSearch,
    ExploreSearchChar(char),
    ExploreSearchBackspace,
    ExploreSearchCancel,
    ExploreSearchCommit,
    /// `Enter` on a row: open the trace-detail view of the
    /// selected span's root.
    ExploreOpenTrace,
    /// `Esc` from trace-detail: back to explore.
    ExitTraceDetail,
    TraceDetailSelectUp,
    TraceDetailSelectDown,
    /// Right / `l`: expand the selected span's subtree.
    TraceDetailExpand,
    /// Left / `h`: collapse the selected span's subtree.
    TraceDetailCollapse,
    /// `s`: jump to the stacks table from any other view.  Pops
    /// out of Explore / TraceDetail with the level restore that
    /// `ExitExplore` would do.
    EnterTable,
    /// `g` from any non-Graph view: lock onto the current view's
    /// stack (cursor row for Table, locked_stack for Explore /
    /// TraceDetail) and open the graph.
    EnterGraph,
    /// `Tab` inside graph mode.  Swaps focus between Chart and
    /// Details panes (graph-mode analogue of `SwitchFocus`).
    GraphSwitchFocus,
    /// `j`/`k` inside graph Details.  Moves the split-key candidate
    /// cursor.
    GraphSelectUp,
    GraphSelectDown,
    /// `Space` inside graph Details.  Toggles the currently-cursor
    /// split key.  Wipes the store because the series partitioning
    /// changed.
    GraphToggleSplit,
    /// `Left` / `Right` inside graph Details.  Cycles the active
    /// sort column for the series table.  Underlined in the header
    /// of the expanded details pane.
    GraphSortColumnLeft,
    GraphSortColumnRight,
    Connected,
    Disconnected(String),
    Status(String),
    Quit,
    /// `v`: when the server and client crate versions disagree,
    /// open a y/n modal asking whether to re-run the installer
    /// pinned to the server's version.  No-op otherwise.
    BeginConfirmVersionSwitch,
    /// `y` in the confirm-version modal: close it and emit
    /// [`Effect::RunUpdateInstaller`] pinned to the server's version.
    ConfirmVersionSwitchYes,
    /// `n` / `Esc` in the confirm-version modal: close it, no effect.
    /// Ignored while the installer is mid-run (`ConfirmStatus::Running`).
    ConfirmVersionSwitchNo,
    /// `Esc` while the "press q again to quit" prompt is up.
    /// Clears `quit_confirm_deadline` without quitting.
    QuitConfirmDismiss,
    /// The async installer task finished with a successful exit
    /// status.  The currently-running binary is now stale, so the
    /// reducer returns `Effect::Quit`.
    InstallerSucceeded,
    /// The async installer task failed — exit status non-zero, or
    /// the subprocess couldn't be launched at all.  Carries the
    /// captured stdout+stderr (best-effort merged) so the modal
    /// can show what went wrong.
    InstallerFailed(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    None,
    Quit,
    /// User committed a tentative level selection (Enter on the Level
    /// pane).  The runtime translates this into an outgoing
    /// `SetCacheLevel` RPC.  The model itself does not update
    /// `cache_level` — that only flips when the server confirms.
    RequestSetLevel(WireLevelFilter),
    /// User committed a chance-input buffer.  The runtime turns this
    /// into an outgoing `SetCacheChance` RPC.  `cache_chance` does
    /// not change locally — the server's `CacheChance` confirms.
    RequestSetChance(f64),
    /// Tear down the TUI and re-run the public installer (`curl |
    /// bash`), pinned to `version` when `Some` or "latest" when
    /// `None`.  Used by `--update` and by the in-app
    /// version-mismatch confirm modal.  The runtime exits once the
    /// installer finishes so the user can `exec` the upgraded
    /// binary.
    RunUpdateInstaller {
        version: Option<String>,
    },
    /// Exec the installed binary in place of this process — same
    /// argv, same env, same controlling terminal.  Emitted by the
    /// post-install Restart confirm.  The runtime tears down the
    /// TUI before exec'ing so the new process starts with a clean
    /// terminal state.
    Restart,
}
