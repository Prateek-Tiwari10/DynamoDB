#!/usr/bin/env bash
# One-time setup per laptop, after cloning the repo. Run from the repo root:
#   bash .team/setup.sh Prateek      (or Meet)
set -e
name="${1:?usage: bash .team/setup.sh Prateek|Meet}"
[ -d .git ] && [ -d .team ] || { echo "Run from the repo root"; exit 1; }

echo "$name" > .team/me          # who am I (gitignored by .team/.gitignore)
chmod +x .team/hooks/sync.sh     # SessionStart hook is registered in .claude/settings.json

echo "Done. Working as $name. Check your git identity (commits must be yours):"
echo "  $(git config user.name) <$(git config user.email)>"
echo "If wrong: git config --global user.name 'Your Name' && git config --global user.email you@example.com"
