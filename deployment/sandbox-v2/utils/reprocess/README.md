# Reprocess packets from securezone

## Purpose
Registration Processor has a Reprocessor stage that resumes stuck packets automatically on a schedule. This script is for restarting packets **manually**: it sends registrations to the Registration Processor **securezone notification API**, so that they restart from the securezone stage.

The script is generic. It sends every registration returned by the query in `config.py` (`--db`), listed in a file (`--file`) or given on the command line (`--registration`). Which packets to restart is decided by the query or the input, not by the script.

## Prerequisites
### Install
Requires Python 3.6 or later. The only extra packages are `requests` and `psycopg2-binary` (`requirements.txt`).

**Linux and macOS:**
```
$ ./preinstall.sh
$ source venv/bin/activate
```
`preinstall.sh` creates a virtual environment in `./venv` and installs the packages into it. Nothing is installed system-wide. If Python 3 or its `venv` module is missing, it stops and prints the install command for your system. On Ubuntu/Debian this is usually `sudo apt-get install -y python3 python3-venv`.

Activate the environment (`source venv/bin/activate`) in every new shell before running the script.

**Windows** (Command Prompt):
```
> py -m venv venv
> venv\Scripts\activate
> pip install -r requirements.txt
```

### Environment variables

| Variable | Description |
|---|---|
| `REGPROC_CLIENT_PWD` | Secret of the `mosip-regproc-client` client |
| `DB_USER`, `DB_PWD`, `DB_HOST`, `DB_PORT` | Access to the `mosip_regprc` database (needed for `--db` only) |

Set them in the same shell that runs the script. They are lost when the shell is closed.

**Linux and macOS:**
```
$ export REGPROC_CLIENT_PWD='<mosip-regproc-client secret>'
$ export DB_USER='<db user>'
$ export DB_PWD='<db password>'
$ export DB_HOST='<db host>'
$ export DB_PORT='5432'
$ ./reprocess.py --db --dry-run
```

**Windows** (Command Prompt, no quotes around the values):
```
> set REGPROC_CLIENT_PWD=<mosip-regproc-client secret>
> set DB_USER=<db user>
> set DB_PWD=<db password>
> set DB_HOST=<db host>
> set DB_PORT=5432
> python reprocess.py --db --dry-run
```

## Configuration (`config.py`)
| Setting | Default | Description |
|---|---|---|
| `server` | `''` | Base URL, e.g. `https://api-internal.sandbox.mosip.net`. Can be overridden with `--server`. |
| `ssl_verify` | `True` | Set `False` for self-signed certificates, or use `--disable_ssl_verify`. |
| `client_id` | `mosip-regproc-client` | Client used to log in. Its secret comes from `REGPROC_CLIENT_PWD`. |
| `app_id` | `admin` | Application id sent with the login request. |
| `login_with_client_secret` | `True` | Login method. `True`: client id and secret (`/v1/authmanager/authenticate/clientidsecretkey`). `False`: user name and password (`/v1/authmanager/authenticate/useridPwd`), with `client_id` as the user name and `client_pwd` as the password. |
| `query` | See the **Query** section below | SQL used by `--db`. |
| `delay` | `1` | Seconds between two registrations. Lower to go faster, raise to reduce load. |
| `timeout` | `60` | HTTP timeout per call, in seconds. |
| `db_connect_timeout` | `30` | Seconds to wait for a database connection (`--db`). A new connection is opened for each batch. |
| `max_retries` | `3` | Retries for network errors and HTTP 5xx. |
| `backoff` | `[5, 10, 20]` | Seconds to wait before retry 1, 2, 3. |
| `max_consecutive_failures` | `10` | Stop the run after this many failures in a row. |
| `failed_file`, `remaining_file` | `./failed.txt`, `./remaining.txt` | Output files. A timestamp is added to each name per run. |

## Query
### Contract
Any query can be used. It must return these columns, **in this order**:

| Column | Sent to securezone as |
|---|---|
| `reg_id` | `rid` |
| `process` | `reg_type` |
| `workflow_instance_id` | `workflowInstanceId` |
| `iteration` | `iteration` |
| `source` | `source` |

### Changing the query
The query is the `query` setting in `config.py`. Validate it before use, and change it as per your requirement, as long as it returns the columns in the **Contract** above.

To use a different query:
1. Edit `query` in `config.py`. Keep the 5 columns above, in the same order.
2. Count the rows: run it with `count(*)` in place of the column list and without `LIMIT`.
3. See the rows: `./reprocess.py --db --dry-run`.
4. On a large table, check the plan: `ANALYZE regprc.registration;` then `EXPLAIN (ANALYZE, BUFFERS) <query>`. Avoid `Seq Scan on registration`.
5. Run `./reprocess.py --db` (or `--db --loop`).

**Note:** always keep a `LIMIT` in the query (e.g. `LIMIT 1000`). The script loads every row the query returns into memory before sending, so a query without `LIMIT` can load the whole table. Use `--loop` to go through all matching packets batch by batch.

## Options
| Option | Description |
|---|---|
| `--db` | Fetch registrations with `config.query`. |
| `--file <path>` | Text file that you create, any name, one registration per line: `rid, process, workflow_instance_id, iteration, source`. All 5 fields are required, in this order (the same as the query columns). Take the values from `regprc.registration`. `source` may be empty but its field must be present (e.g. `..., 1, `). Quotes are optional. Empty lines and lines starting with `#` are ignored. A `failed_<ts>.txt` or `remaining_<ts>.txt` from an earlier run is already in this format and can be passed directly. |
| `--registration "<line>"` | One registration given directly on the command line, no file needed. Same 5 fields as a `--file` line, in double quotes. |
| `--loop` | With `--db` only. Repeats until the query returns no rows: run the query, send the rows, run the query again. <br><br>**Tracking:** the script remembers every packet it sends in the run (success or failure), using `workflow_instance_id` as the unique identifier. If a later batch returns a packet with the same `workflow_instance_id`, it is skipped, so a packet is sent at most once per run. If a batch contains only packets already sent, the run stops. The tracking is in memory only: if the script is run again (a new run), the list starts empty, and a packet the query still returns is sent again. <br><br>**Query:** must not return a packet once it is sent, otherwise the run stops after the first batch. Use `latest_trn_dtimes < '<run start time>'`, e.g. `latest_trn_dtimes < '2026-11-10 10:00'`: securezone sets `latest_trn_dtimes` to the current time on a sent packet, so it drops out of the next batch. |
| `--dry-run` | Prints the registrations and sends nothing. With `--db`: runs the query once and prints the rows it returns (at most the query's `LIMIT`, e.g. 1000), even with `--loop`. With `--file` / `--registration`: prints every registration given. |
| `--server <url>` | Overrides `server` in `config.py`. |
| `--disable_ssl_verify` | Disables SSL certificate verification. |

`--db`, `--file` and `--registration` are mutually exclusive, and one of them is required.

## Examples
```
# See what would be sent
$ ./reprocess.py --db --dry-run

# One batch
$ ./reprocess.py --db

# All batches until the query returns no rows
$ ./reprocess.py --db --loop

# Single registration
$ ./reprocess.py --registration "10001101040000520220809113748, NEW, e6e944b1-5c4b-42aa-b749-b39428cb7512, 1, REGISTRATION_CLIENT"

# Single registration without source (source is NULL in regprc.registration):
# keep the comma after iteration and leave the last field empty
$ ./reprocess.py --registration "10001101040000520220809113748, NEW, e6e944b1-5c4b-42aa-b749-b39428cb7512, 1, "

# Registrations from a file
$ ./reprocess.py --file registrations.txt

# Retry the failures of a run
$ ./reprocess.py --file failed_20261106_101500.txt
```

Example `registrations.txt`:
```
# rid, process, workflow_instance_id, iteration, source
10001101040000520220809113748, NEW, e6e944b1-5c4b-42aa-b749-b39428cb7512, 1, REGISTRATION_CLIENT
'10001101040000520220809113730', 'UPDATE', 'e6e944b1-5c4b-42aa-b749-b39428cb7333', 1, 
```

**Empty `source`:** when `source` is NULL in `regprc.registration`, keep all 5 fields and leave the last one empty, e.g. `"<rid>, NEW, <workflow_instance_id>, 1, "`. The trailing comma is required: `"<rid>, NEW, <workflow_instance_id>, 1"` has only 4 fields and is rejected. An empty `source` is sent to securezone as null.

## Error handling
| Error | Action |
|---|---|
| Securezone says the packet was not forwarded (e.g. "has not been uploaded to file System") | Marked failed. Run continues. |
| HTTP 4xx (other than 401) | Marked failed. Run continues. |
| HTTP 401 (token expired) | Logs in again and retries once. If the login fails, the run stops. |
| HTTP 5xx | Retried `max_retries` times with `backoff`. Then marked failed. Run continues. |
| Malformed line or row (not exactly 5 fields, a required value missing, `iteration` not a number) | Marked failed. Run continues. |
| Network error (connection refused, timeout, SSL, DNS) | Retried `max_retries` times with `backoff`. Then **the run stops**. |
| Authentication fails at start | **The run stops.** |
| Database connection or query fails (`--db`) | **The run stops.** A new connection is opened for each batch and closed straight after the query, so nothing is held open while a batch is sent. Rerunning continues where it stopped. |
| `max_consecutive_failures` failures in a row | **The run stops.** This usually means a system-wide problem. |

A registration is successful only when securezone answers "has been forwarded to next stage". Securezone returns HTTP 200 for both outcomes, so the script checks the message text.

## Exit codes
| Code | Meaning | What to do |
|---|---|---|
| `0` | Every registration was forwarded, or there was nothing to process. | Nothing. |
| `1` | Run stopped: network error, authentication failure, too many consecutive failures, invalid arguments or unexpected error. | Read the `RUN STOPPED` line in the log and fix the cause. Then rerun: with `--db`, the same command (unsent rows are still returned by the query); with `--file`, use `--file remaining_<ts>.txt`. |
| `2` | Run finished, but some registrations failed. | Check `failed_<ts>.txt` (each line is preceded by the reason) and rerun with `--file failed_<ts>.txt` once fixed. |

## Output
| File | Content |
|---|---|
| `out.log` | All runs (appended). One line per registration: `n/total | rid | process | workflow_instance_id | iteration | source | SUCCESS/FAILED | message`, then a summary. Also printed on the console. |
| `last.log` | Last run only. |
| `failed_<ts>.txt` | Failed registrations in `--file` format, each preceded by `# <reason>`. Created only if something failed. |
| `remaining_<ts>.txt` | Registrations not yet sent when a `--file` / `--registration` run stops. In `--db` mode it is not needed: unsent rows are returned by the query again. |

Summary line: `batches=<n> picked=<n> success=<n> failed=<n>`.
