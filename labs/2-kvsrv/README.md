# Lab 2: Key/value server

One server with `Get`, `Put`, and `Append`, reached over an unreliable
network.

```sh
go run . 2
go test -race ./labs/2-kvsrv/
```

## The problem

The network can lose a request *or* a reply. To the client both look the
same: no answer. So the client must retry. But if only the reply was lost,
the server already applied the request, and applying a retried `Append`
again would duplicate data.

## The fix: at-most-once semantics

- Each clerk picks a random `ClientId` and numbers its requests with `Seq`.
  Every retry of a request carries the same `Seq`.
- The server remembers, per client, the last `Seq` it applied and the reply
  it sent. If a request comes in with `Seq <=` that, it's a duplicate: the
  server returns the saved reply without applying it again.
- A clerk only has one request outstanding at a time, so once it sends
  `Seq` n+1 it has received the reply to n. The server only needs to keep
  **one** record per client, not one per request.

`Get` doesn't change anything, so repeating it is harmless and it needs no
deduplication.

The same idea appears again in labs 4 and 5, where the dedup table becomes
part of the replicated state.
