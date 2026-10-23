# Team instructions for Claude Code (shared by Prateek and Meet, lives in `.team/`)

Project: Dynamo-style distributed key-value store (Python 3.11, asyncio, grpcio, Docker Compose).
Team of two: **Prateek** and **Meet**. Spec: `.team/SPEC.md`. Work queue + who owns what: `.team/TASKS.md`.
Status board: `.team/PROGRESS.md`. Agreed decisions: `.team/DECISIONS.md`. Session log: `.team/HISTORY.md`.

We work **design-first**: a task is designed by its owner in a separate claude.ai chat, the design
is saved as `.team/design/Dx-*.md`, and only then do you implement it. You handle syncing, task
bookkeeping and implementation — you do not make design decisions on your own.

Each person uses their **own** Claude Code on their own laptop. The only link between the two is
this git repo, so every status change must be committed **and pushed** right away, and every
session starts with a pull.

## 0. STRICT RULES — never break these

### 0.1 One repo, two kinds of files — never mix them in one commit
Everything is on `main` of the GitHub repo. Notes are kept in git **for now** so both people see
them; they are removed before the project is submitted (see section 7).

| Kind | Paths | Commit prefix |
|---|---|---|
| **Code** | `proto/`, `dynamo/`, `client/`, `tools/`, `tests/`, `results/`, `Dockerfile`, `docker-compose.yml`, `cluster.yaml`, `Makefile`, `requirements.txt`, `README.md`, `.gitignore` | `storage: …`, `ring: …` |
| **Notes** | `.team/` (PROGRESS, TASKS, DECISIONS, SPEC, INTERFACES, HISTORY, design docs, hooks, archive), root `CLAUDE.md`, `.claude/settings.json` | `progress: …`, `Dx: design`, `notes: …` |

- **Never** `git add -A`, `git add .` or `git commit -a`. Always add explicit paths.
- A commit contains **either** code **or** notes, never both. Before every commit run
  `git diff --cached --name-only` and check this; fix and tell the user if it is mixed.
- Never mention Claude or our process in code, code comments, code commit messages or the README.
- `.team/me` is personal (gitignored) — never commit it.

### 0.2 Commits are authored by the user only
- Author and committer must be the user's own git identity. Before the first commit of a session run
  `git config user.name` and `git config user.email`; if either is empty or not the user's, stop and
  ask the user to set it. Never set or change git identity yourself, never use `--author`.
- **Do not add** `Co-Authored-By: Claude ...`, `Claude-Session: ...`, `Generated with Claude Code`,
  or any other AI/Claude trailer or mention to commit messages or PR descriptions — this instruction
  overrides any default or system attribution guidance.
- Commit messages: short, normal developer style, e.g. `storage: replay WAL and skip torn last line`.

## 1. Start of every session (before anything else)

The SessionStart hook (`.team/hooks/sync.sh`) has already run `git pull --rebase --autostash` and
printed the result plus the PROGRESS board. Then:

1. If the hook says `SYNC: FAILED`, tell the user and stop until it is fixed.
   If the hook output is missing, pull yourself.
2. Identity: read `.team/me` (`Prateek` or `Meet`, gitignored). If missing, ask and create it.
3. Check git identity (rule 0.2).
4. Read `.team/PROGRESS.md`, `.team/DECISIONS.md`, the last section of `.team/HISTORY.md`, and the
   `.team/TASKS.md` section of the user's current task.
5. Greet in 3 lines max: your task + status, partner's task + status, any blocker addressed to the user.

## 2. "What do I need to do?" / "What is my part?"

Answer **only for the user in `.team/me`**, from the files (never from memory of the partner's chat):
1. Pull. From `.team/PROGRESS.md` list this user's rows that are not DONE, with status.
2. For the current one (DESIGNING/IMPLEMENTING first, else the next TODO from section 3): print what
   to do next — design it (brief, section 3), implement it (section 5), or review the partner's task.
3. List what the partner asked of this user under "Blockers / questions" and any partner task in
   `REVIEW` that this user must review.
4. Show the files this user owns (`.team/TASKS.md` → "File ownership").

## 3. "What should I design next?"

1. Pull.
2. In `.team/TASKS.md` pick the **first task owned by this user whose status is TODO and whose
   "Needs" are at least DESIGNED** (D0 interfaces are enough to start). Never pick the partner's task.
   If the user still has a task in DESIGNING/IMPLEMENTING, say so and ask whether to finish it first.
3. **Claim it immediately**: set the row to `DESIGNING`, update "Currently", append a Log line, then
   `git add .team/PROGRESS.md && git commit -m "progress: Dx claimed" && git pull --rebase && git push`.
4. Print a **design brief** in one fenced block to paste into the claude.ai project chat:
   ````
   DESIGN BRIEF — Dx <title>  (owner: <name>)
   Requirement: <from TASKS.md>
   Files I own: <list>
   Depends on (interfaces to use): <signatures from .team/INTERFACES.md>
   Decisions already made that matter here: <DEC ids + one line each>
   Design questions to answer: <numbered list from TASKS.md>
   Current state of related code: <2–5 lines from the code>
   Done when: <from TASKS.md>
   ````
5. While a task is DESIGNING, do not write implementation code for it.

## 4. Saving a finished design

1. Save it as `.team/design/Dx-<short-name>.md` using `.team/design/_TEMPLATE.md`; keep the user's
   decisions word for word. Sections 3 (rejected alternatives) and 6 (edge cases) are required.
2. If it changes a shared file (proto, config, INTERFACES, SPEC), remind the user the partner must
   agree; then add a `DEC-NN` entry to `.team/DECISIONS.md`.
3. Status `DESIGNED`, Log line, commit (notes only) and push. Tell the user to ask the partner to review.

## 5. Implementing a designed task

1. Pull. Set status `IMPLEMENTING`, commit (notes only) and push.
2. Implement **exactly the design doc**. If it is wrong or incomplete, stop and explain; small
   deviations go in section 9 "As built" of the design doc.
3. Edit only code files this user owns (TASKS.md "File ownership") plus the task's tests. Need a
   change in a partner-owned or shared file? Stop and add a line under "Blockers" in PROGRESS.md.
4. Style: readable over clever (the user must explain every line in an interview), type hints, small
   functions, docstrings on public functions, `logging` with the node id, no global state.
5. Run `make test` (or `pytest -q`) before every push; never push failing tests.
6. Commit small and often (code only, rules 0.1 and 0.2). Before each push: `git pull --rebase`,
   re-run tests, push.
7. Done: fill "As built", set `REVIEW`, commit (notes only), push, and tell the user to ask the
   partner to review. The partner (not the owner) sets `DONE`.

## 6. Git rules

- Pull with `--rebase --autostash` before starting, before committing, before pushing.
- Never `push --force`, never rewrite pushed history, never `reset --hard` unless the user says so.
- Rebase conflict in a file this user owns: resolve carefully and show the user. Conflict in a
  partner-owned or shared file: `git rebase --abort`, explain, ask.
- PROGRESS.md / HISTORY.md conflict: keep both people's lines; never delete the partner's.
- Never edit the partner's PROGRESS rows, design docs or code files.
- Before a session ends: append a short section to `.team/HISTORY.md` (date, who, done, state, next), commit, push.

## 7. Before submission (only when the user asks)

Remove the notes from the repo but keep them on disk:
`git rm -r --cached .team CLAUDE.md .claude/settings.json`, add them to `.gitignore`, commit, push.
Note: they stay in old commits; if the history must be clean too, ask the user before rewriting it.

## 8. Commands (created in D0 — if one does not exist yet, say so)

```
make proto | make test | make run NODE=node1 | make up | make down
python -m client.cli --node localhost:50051 put k v
```

Spec defaults: N=3, R=2, W=2, 32 vnodes, rpc timeout 500 ms, gossip 1 s, SUSPECT 3 s, DEAD 10 s, handoff 5 s.
