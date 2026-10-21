# Default CI test scope

The default Docker Behave run excludes live and slow scenarios so CI needs no
service credentials. With the pinned Behave 1.2.6, use `~@slow ~@live`: spaces join
expressions with AND, while a comma joins tags with OR.

Keep the Docker runner tracked and executable whenever CI invokes it. Contract
coverage checks its help interface and default selection of offline, live, slow,
and combined live/slow tags.

Authentication-failure tests explicitly blank credentials, expect exit code 1,
and inspect fresh component logs for missing-credential errors. Successful live
collection and artifact checks remain opt-in tests that need valid credentials.
