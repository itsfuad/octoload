# Contributing

Thank you for considering contributing to this project! Here are some guidelines to help you get started:

## Code Structure

Please make sure to follow the existing code structure and organization. This helps maintain consistency and readability across the project.

## Testing

Ensure that all tests pass before submitting your changes. You can run the tests using the following command:

```sh
go test -race ./...
```

CI runs tests on both the Go version in `go.mod` and the latest stable release, because `net/http` behavior differs between them. Keep `go.mod` and the first entry of the CI matrix in line.

## Formatting

Make sure to format your code before committing. You can format your Go files using the following command:

```sh
gofmt -w .
```

## Pull Requests

When submitting a pull request, please include a clear description of the changes and the motivation behind them. Also, make sure to reference any relevant issues.

Thank you for your contributions!