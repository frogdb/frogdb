# VS Code configuration template

Install with:

```bash
just vscode-setup
```

`root/` lands in `.vscode/`, `website/` lands in `website/.vscode/`. Both destinations
are git-ignored; edit them freely, and copy anything worth sharing back here.

## Why it is a template instead of tracked config

The Claude Code sandbox denies writes to any `.vscode` path, and nothing in a settings file
lifts that deny — naming the directory exactly in `sandbox.filesystem.allowWrite` does not
work, allowing its parent does not work, and the list does not expand wildcards. All three
were tested. While these files were tracked, every `git worktree add` aborted:

```
error: unable to create file website/.vscode/launch.json: Operation not permitted
fatal: Could not reset index file to revision 'HEAD'
```

Untracked files are never checked out, so the deny has nothing to act on.
