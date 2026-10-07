# echo-api

Starter HTTP API on [Echo](https://echo.labstack.com/) v4. It is its own Go
module, so its dependencies stay out of the labs module.

## Run

```sh
cd echo-api
go run .                 # listens on :8080 (override with PORT=9000)
curl localhost:8080/health
curl "localhost:8080/hello?name=Go"
go test ./...
```

## Layout

```
main.go     starts the server, shuts down gracefully on Ctrl+C / SIGTERM
server.go   newServer(): middleware and routes, plus the handlers
```

Add routes in `newServer` and test them through `e.ServeHTTP` as in
`server_test.go`.
