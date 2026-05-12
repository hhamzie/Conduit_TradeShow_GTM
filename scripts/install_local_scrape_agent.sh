#!/bin/zsh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$REPO_DIR/.venv/bin/python"
AGENT_SCRIPT="$REPO_DIR/scripts/local_scrape_agent.py"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS_DIR/com.conduit.localscrape.plist"
LOG_DIR="$HOME/Library/Logs/Conduit"
STDOUT_LOG="$LOG_DIR/local_scrape_agent.out.log"
STDERR_LOG="$LOG_DIR/local_scrape_agent.err.log"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python virtualenv at $PYTHON_BIN"
  exit 1
fi

mkdir -p "$LAUNCH_AGENTS_DIR"
mkdir -p "$LOG_DIR"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.conduit.localscrape</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PYTHON_BIN</string>
    <string>$AGENT_SCRIPT</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$REPO_DIR</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$STDOUT_LOG</string>
  <key>StandardErrorPath</key>
  <string>$STDERR_LOG</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
  </dict>
</dict>
</plist>
EOF

launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl load "$PLIST_PATH"

echo "Installed local scrape agent launch service."
echo "It will now start automatically on login and stay available at http://127.0.0.1:8765."
