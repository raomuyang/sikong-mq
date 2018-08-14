.PHONY: compile build

test:
	@echo "Test project"
	go test -v -race ./skmq/...
	go vet ./...

build:
	go build
compile:
	mkdir -p target
	GOOS=darwin GOARCH=amd64 go build  -o target/sikong-mac sikong.go
	GOOS=linux GOARCH=amd64 go build  -o target/sikong-linux sikong.go
	GOOS=windows GOARCH=amd64 go build  -o target/sikong-win.exe sikong.go
	chmod a+x target/*
