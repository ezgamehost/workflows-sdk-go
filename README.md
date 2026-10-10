# EZGH Workflows Go SDK

Durable workflows in your own Go app. You write workflows as ordinary Go functions made of
steps; EZGH Workflows records each step's result, retries failures, sleeps for as long as you
like and resumes the workflow where it left off, invoking your app over HTTP (the push handler)
or through a pull worker. The SDK uses only the Go standard library.

**Status:** in development.

```sh
go get github.com/ezgamehost/workflows-sdk-go
```

## Licence

Apache-2.0. See [LICENSE](LICENSE).
