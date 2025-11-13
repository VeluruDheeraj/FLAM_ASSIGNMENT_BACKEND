# QueueCTL

QueueCTL is a lightweight CLI tool for running shell commands as background jobs on a single machine. It offers:

- Persistent storage (SQLite)
- Worker processes with locking to avoid duplicate work
- Automatic retries with exponential backoff
- Dead Letter Queue (DLQ) for permanently failed jobs

This README provides quick installation steps, common commands, examples, and troubleshooting notes.

---

## Quick Start

Prerequisites:

- Python 3.8+
- pip

Install dependencies:

```powershell
pip install -r requirements.txt
```

Run basic commands from the repository root:

```powershell
# Enqueue a simple job (plain text or JSON accepted)
python -m queuectl.cli enqueue "echo Hello World"

# Start a single worker (foreground)
python -m queuectl.cli worker start

# Start 3 workers
python -m queuectl.cli worker start --count 3

# Show queue status
python -m queuectl.cli status

# List pending jobs
python -m queuectl.cli list --state pending

# Show DLQ entries
python -m queuectl.cli dlq list
```

If you installed the package editable (`pip install -e .`) you can run `queuectl` directly.

---

## Commands (summary)

- `enqueue [JSON_OR_TEXT]` — add a job; accepts JSON or plain command text.
- `worker start [--count N] [--poll-interval S]` — start N workers.
- `status` — show queue summary and DLQ size.
- `list [--state STATE] [--limit N]` — list jobs filtered by state.
- `show JOB_ID` — show job details.
- `delete JOB_ID` — delete a job from the main queue.
- `dlq list|retry|remove` — inspect, retry, or remove DLQ entries.
- `config get|set KEY [VALUE]` — view or change runtime configuration.

Run `python -m queuectl.cli --help` for full options and subcommand help.

---

## Job JSON (optional)

You may enqueue a job with JSON to set additional metadata. Example:

```json
{
  "id": "optional-unique-id",
  "command": "the shell command to run",
  "max_retries": 3
}
```

If `id` is omitted the CLI generates one. Passing plain text enqueues that string as the `command`.

---

## Storage & Configuration

- Database: `~/.queuectl/jobs.db` (SQLite)
- Config: `~/.queuectl/config.json`

Key config options (defaults):

- `max_retries`: 3
- `backoff_base`: 2
- `backoff_max_seconds`: 600

Changing config affects new jobs; existing jobs keep their stored `max_retries`.

---

## Workers (brief)

- Workers poll for ready jobs and acquire a database lock (`locked_until`) before running a job.
- Failed jobs are retried with exponential backoff until `max_retries` is reached.
- Jobs that exceed retries are moved to the DLQ for manual inspection or retry.

---

## Examples (PowerShell)

Enqueue a plain command:

```powershell
python -m queuectl.cli enqueue "echo Hello from QueueCTL"
```

Enqueue using JSON (PowerShell: prefer single quotes around JSON):

```powershell
python -m queuectl.cli enqueue '{"command":"timeout /t 2 & echo done","max_retries":2}'
```

Start a single worker (foreground):

```powershell
python -m queuectl.cli worker start
```

Start a worker in background with `Start-Process`:

```powershell
Start-Process -FilePath python -ArgumentList '-m queuectl.cli worker start --count 1' -WindowStyle Hidden
```

Check status and list pending jobs:

```powershell
python -m queuectl.cli status
python -m queuectl.cli list --state pending
```

Inspect DLQ and retry a DLQ entry:

```powershell
python -m queuectl.cli dlq list
python -m queuectl.cli dlq retry <dlq_job_id>
```

Quoting tips for PowerShell:

- Use single quotes to pass JSON so you don't escape double quotes.
- For complex shell commands, wrap the whole command in double quotes when passing as plain text.

---

## Troubleshooting

- Jobs stuck in `processing`: locks will expire automatically; check `queuectl status` and `queuectl list --state processing`.
- Jobs not processed: confirm worker processes are running.
- SQLite "database is locked": ensure no heavy concurrent writers; the code uses a connection timeout.

---

## Tests & Demo

Run tests:

```powershell
python tests.py
```

Run demo:

```powershell
python demo.py
```

---

## Caveats & Security

- Single-machine/local usage only.
- Commands execute in a shell — do not enqueue untrusted input.
- Data is stored unencrypted on disk.

---

If you'd like an `examples/` folder with runnable scripts and a `start-workers.ps1` helper, I can add it—tell me how many workers you want the sample to start.
# QueueCTL

QueueCTL is a lightweight CLI-driven background job queue for running shell commands reliably on a single machine. It provides persistent storage (SQLite), workers with locking, automatic retries with exponential backoff, and a Dead Letter Queue (DLQ) for permanently failed jobs.

This README covers installation, common commands, configuration, and troubleshooting to get you started quickly.
## 🎥 CLI Demo

[▶️ Watch the CLI demo here](https://drive.google.com/file/d/1lMGIzJIbGzYjoAbaz4kblusLSsNONwO3/view?usp=drive_link)




---
