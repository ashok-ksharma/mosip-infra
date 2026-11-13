import os

# ---------------------------------------------------------------------------
# Server
# Base URL of the MOSIP internal API gateway. The script adds the API path to it:
#   login      : <server>/v1/authmanager/authenticate/clientidsecretkey
#   securezone : <server>/registrationprocessor/v1/securezone/notification
# Example for the sandbox environment:
#   server = 'https://api-internal.sandbox.mosip.net'
#   -> https://api-internal.sandbox.mosip.net/registrationprocessor/v1/securezone/notification
# Use the internal gateway (api-internal.<domain>): securezone and the regproc client
# login are normally exposed only there. No trailing slash.
# Leave empty only for --dry-run. Can be overridden with --server.
# ---------------------------------------------------------------------------
server = ''

# Verify the server's SSL certificate. True or False (capitalised, no quotes).
# Set False for self-signed certificates, or use --disable_ssl_verify.
ssl_verify = True

# ---------------------------------------------------------------------------
# Credentials (read from environment variables, see README)
# ---------------------------------------------------------------------------
# Client used to log in to the MOSIP auth manager, and the application id sent with the login request
client_id = 'mosip-regproc-client'
client_pwd = os.getenv('REGPROC_CLIENT_PWD')
app_id = 'admin'

# Login method. True or False (capitalised, no quotes).
#   True : client id and secret   -> <server>/v1/authmanager/authenticate/clientidsecretkey
#   False: user name and password -> <server>/v1/authmanager/authenticate/useridPwd
#          (client_id is then used as the user name and client_pwd as the password)
login_with_client_secret = True

db_user = os.getenv('DB_USER')
db_pwd = os.getenv('DB_PWD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = 'mosip_regprc'

# Seconds to wait for a database connection before giving up (one connection is opened per batch)
db_connect_timeout = 30

# ---------------------------------------------------------------------------
# Run settings
# ---------------------------------------------------------------------------
# Seconds to wait between two registrations
delay = 1

# HTTP timeout (seconds) for each API call
timeout = 60

# Retries for network errors and HTTP 5xx, and the wait (seconds) before each retry.
# backoff needs at least one value. If there are more retries than values, the last value is reused.
max_retries = 3
backoff = [5, 10, 20]

# Stop the run when this many registrations fail one after another
max_consecutive_failures = 10

# Output files
failed_file = './failed.txt'
remaining_file = './remaining.txt'

# ---------------------------------------------------------------------------
# Query used by --db. Every row it returns is sent to the securezone notification API.
# Change the conditions as per the use case. The query MUST return these columns,
# in this order:
#   reg_id, process, workflow_instance_id, iteration, source
# Add a LIMIT to process in batches.
# Default: unfinished packets that have not moved for more than a day.
# ---------------------------------------------------------------------------
query = """
SELECT reg_id, process, workflow_instance_id, iteration, source
FROM regprc.registration
WHERE latest_trn_status_code IN ('SUCCESS', 'REPROCESS', 'IN_PROGRESS')
  AND reg_process_retry_count <= 500
  AND latest_trn_dtimes < (SELECT NOW() - INTERVAL '1 DAY')
  AND status_code NOT IN ('PROCESSED', 'FAILED', 'REJECTED')
LIMIT 1000
"""
