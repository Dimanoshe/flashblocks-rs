#!/usr/bin/env bash
# One-minute terminal demo of the flashblocks crate, recorded with asciinema and rendered to docs/demo.gif with agg:
#   asciinema rec --window-size 132x24 --overwrite -c "bash docs/demo.sh" docs/demo.cast
#   agg --font-size 14 --idle-time-limit 4 docs/demo.cast docs/demo.gif
# Needs the release binary (cargo build --release) and a warm `cargo test` build so the last step takes seconds.
set -u
cd "$(dirname "$0")/.."
export PATH="$PWD/target/release:$PATH"
CBETH_USD=0x71e021bc2e8a709b72ac7b6036e5b2bf30f263d0
BTC_USD=0x92f88cca9d3c846be7e8163e4c32bb07772cd245
AERO_USD=0xfcddfb2294beba43987516371febb2e677eb403e

say() { printf '\033[1;36m$ %s\033[0m\n' "$*"; sleep 1.2; }
note() { printf '\033[2m# %s\033[0m\n' "$*"; sleep 1.5; }

note "flashblocks 0.2: Chainlink price updates on Base from 200 ms blocks (Flashblocks were retired with Denim)"
note "header bloom first, then only the block with the update; skipped headers read by number; confirmed from logs_bloom"
sleep 0.5
note "install: cargo install flashblocks"
say "flashblocks-tail --feed \$BTC_USD --feed \$CBETH_USD --feed \$AERO_USD   # Chainlink aggregators on Base"
# live stream: stop after two updates or 25 minutes, whichever comes first (agg compresses the idle time to seconds)
n=0
perl -e 'alarm 1500; exec @ARGV' -- flashblocks-tail --feed "$BTC_USD" --feed "$CBETH_USD" --feed "$AERO_USD" 2>/dev/null \
  | while IFS= read -r line; do
      printf '%s\n' "$line"
      n=$((n + 1))
      if [ "$n" -ge 2 ]; then pkill -P $$ flashblocks-tail 2>/dev/null; break; fi
    done
sleep 1.5
note "block / tx index / aggregator / new answer / confirmed from logs_bloom / ms from header to line (incl. reading the block)"
sleep 1.5
say "cargo test"
cargo test 2>&1 | grep -E "^test result|Running" | sed 's/^/    /'
sleep 2
note "github.com/Dimanoshe/flashblocks-rs - MIT, Rust crate + CLI + Python module"
sleep 2
