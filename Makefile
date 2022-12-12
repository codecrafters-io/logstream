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

test_produce_local_exit_1: install
	logstream -url=redis://localhost:6379/0/abcd run test_helpers/exit_1.sh

test_append_local: install
	echo "test append" | logstream -url=redis://localhost:6379/0/abcd append

test_produce_large: install
	logstream -url=redis://localhost:6379/0/abcd run test_helpers/large_echo.sh

test_consume_local: install
	logstream -url=redis://localhost:6379/0/abcd follow

test_colors: install
	logstream -url=redis://localhost:6379/0/abcd run echo "\033[31mError\033[0m"