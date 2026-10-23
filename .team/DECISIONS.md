# DECISIONS

Cross-cutting decisions that both people agreed on. Anything that changes a shared file
(proto, config, interfaces, spec) or changes behaviour another task depends on goes here.
Never edit an old entry — add a new one that supersedes it.

Format:

```
## DEC-NN — short title  (YYYY-MM-DD, agreed by Prateek + Meet, from Dx)
Decision: one or two lines.
Why: one line.
Rejected: the alternative and why.
Affects: tasks / files.
```

---

## DEC-01 — Design first, simplest explainable solution  (2026-10-23, both)
Decision: every task is designed and discussed before any code; we pick the simplest correct option we can explain in an interview, not the most optimal one.
Why: same approach as our previous project; interviews reward clear reasoning over cleverness.
Rejected: coding first and designing as we go — leads to rewrites when parts meet.
Affects: all tasks.
