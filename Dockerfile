FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /ephemeral-relay .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /ephemeral-relay /app/ephemeral-relay
ENV DB_PATH=/app/db
EXPOSE 3335
CMD ["/app/ephemeral-relay"]
