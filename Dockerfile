FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO stays on: the lmdb eventstore backend (PowerDNS/lmdb-go) requires it.
RUN go build -o /ephemeral-relay .

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /ephemeral-relay /app/ephemeral-relay
ENV DB_PATH=/app/db
EXPOSE 3335
CMD ["/app/ephemeral-relay"]
