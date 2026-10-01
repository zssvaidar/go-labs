# go-labs

Learning project for Go concurrency and distributed systems, following MIT 6.5840.

## Run

```sh
go run .        # runs lab 1
go run . 2      # runs a lab by number
go run -race .  # check for data races
go test -race ./...
```

## Labs

1. `labs/1-periodic` — stop a periodic goroutine using a mutex-protected `done` flag.
2. `labs/2-raft` — Raft consensus: leader election, log replication, commit, crashes and restarts. See [its README](labs/2-raft/README.md).
