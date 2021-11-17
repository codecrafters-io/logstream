current_version_number := $(shell git tag --list "v*" | sort -V | tail -n 1 | cut -c 2-)
next_version_number := $(shell echo $$(($(current_version_number)+1)))

release:
	git tag v$(next_version_number)
	git push origin master v$(next_version_number)

test:
	go test -v

install:
	go install .

test_produce_local: install
	logstream -url=redis://localhost:6379/0/abcd run echo hey

test_produce_large: install
	logstream -url=redis://localhost:6379/0/abcd run test_helpers/large_echo.sh

test_consume_local: install
	logstream -url=redis://localhost:6379/0/abcd follow
