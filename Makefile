
coverage:
	go test -coverprofile=coverage.out ./... ; go tool cover -html=coverage.out


test:
	rm -f coverage.out coverage.out.tmp
	go clean -testcache
	go test ./... -v -race -coverprofile coverage.out -covermode=atomic
	go tool cover -func coverage.out