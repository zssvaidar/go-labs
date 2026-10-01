# go-labs

Learning project for Go concurrency.

## Run

```sh
go run .        # runs lab 1
go run . 1      # runs a lab by number
go run -race .  # check for data races
```

## Labs

1. `labs/1-periodic` — stop a periodic goroutine using a mutex-protected `done` flag.
