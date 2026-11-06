FROM golang:1.27-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /octoload .

# static:nonroot runs as non-root and has the CA certificates https backends need.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /octoload /octoload
EXPOSE 8080 8081
# Mount your configuration at this path.
ENTRYPOINT ["/octoload", "-config", "/etc/octoload/servers.json"]
