#!/usr/bin/env bash
# Runs at the start of every Claude Code session (registered in .claude/settings.json).
# Pulls the repo (code + .team/ notes are both on main), then prints identity and the PROGRESS board.
# Output is added to Claude's context. Always exits 0 so a sync problem never blocks the session.

root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
cd "$root" || exit 0

echo "=== SESSION SYNC ($(date '+%Y-%m-%d %H:%M')) ==="
me="UNKNOWN"; [ -f .team/me ] && me=$(tr -d '[:space:]' < .team/me)
echo "Working as: $me   (from .team/me — if UNKNOWN run: bash .team/setup.sh <YourName>)"
echo "Git identity: $(git config user.name) <$(git config user.email)>"

if ! git remote get-url origin >/dev/null 2>&1; then
  echo "SYNC: no 'origin' remote — tell the user, nothing was pulled."
elif ! out=$(git pull --rebase --autostash 2>&1); then
  git rebase --abort >/dev/null 2>&1
  echo "SYNC: FAILED — left as before the pull. Tell the user before doing anything else."
  echo "$out" | tail -n 6
else
  echo "SYNC: OK — $(echo "$out" | tail -n 1)"
fi

echo "--- last 5 commits ---"; git log --oneline -5 2>/dev/null
if [ -f .team/PROGRESS.md ]; then
  echo "--- .team/PROGRESS.md (board) ---"
  sed -n '/^## Board/,/^## Log/p' .team/PROGRESS.md | sed '$d'
fi
exit 0
