# Continuing terminal sessions

A Claude Code or Codex session you started in a terminal shows up in T3 Code as
a thread in the project it ran in. The thread is that session, not a copy of it:
messages you send from T3 Code are added to the same session, and turns you run
in the terminal appear in the thread a few seconds later. You can move between
your phone and the terminal on one conversation.

## Threads you started in T3 Code

It works the other way too for Claude: a thread you started in T3 Code is an
ordinary Claude Code session, so `claude --resume <session id>` continues it in
a terminal, and the turns you run there appear in the thread. A Codex thread
started in T3 Code can be resumed in a terminal once T3 Code has let go of it
(up to 30 minutes after its last turn), and terminal turns on it are not shown
in the thread.

## What you need

- The session ran inside a folder you added to T3 Code as a project. A thread
  appears within a few seconds of the session's first reply.
- The T3 Code server runs on the machine that has the session.

## Going back to the terminal

Reopen the session there the usual way:

```bash
claude --resume    # or: claude --resume <session id>
codex resume
```

Everything you sent from T3 Code is in the conversation.

## One side at a time

Only one program can add to a session at once.

- **Claude Code.** If a terminal still has the session open and idle when you
  send from T3 Code, T3 Code closes that terminal and takes the turn. Reopen it
  with `claude --resume`. If the terminal is in the middle of a turn, T3 Code
  refuses and says so; wait for it to finish or stop it there.
- **Codex.** If a terminal has the session open, T3 Code reports that another
  Codex client is using it. Quit Codex there and send again. After a T3 Code
  turn settles, T3 Code lets go of the session within a few seconds so the
  terminal can resume it.

## What the thread shows

Turns that ran in the terminal show your prompts and the agent's replies. Their
tool calls and attachments are not shown. Turns you run from T3 Code show
everything, as in any thread.

The first import keeps the first prompt and the newest 200 messages
([details](./welcome-page.md)); turns added afterwards are not capped.

## Settings

`T3CODE_AGENT_SESSION_SYNC_INTERVAL` sets how often T3 Code looks for new
sessions as a fallback to watching the session folders (default 2 minutes).
`0` turns the whole feature off. Put it in
[`service.env`](./background-service.md#configure-the-service-environment).
