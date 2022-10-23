FROM golang:alpine as builder

WORKDIR $GOPATH/src/github.com/otaviokr/spacetraders-proxy/
COPY web/ web/
COPY go.mod go.mod
COPY go.sum go.sum
COPY main.go main.go

RUN apk --no-cache add ca-certificates && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /go/bin/spacetraders-proxy .

FROM scratch

WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/bin/spacetraders-proxy /app/

ENTRYPOINT ["./spacetraders-proxy"]
