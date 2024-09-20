current_patch_number := $(shell git tag --list "v0.2.*" | sort -V | tail -n 1 | cut -c 6-)
next_patch_number := $(shell echo $$(($(current_patch_number)+1)))

release:
	git tag v0.2.$(next_patch_number)
	git push origin main v0.2.$(next_patch_number)

test:
	REDIS_URL=redis://localhost:6379/testing go test -v ./...

install:
	go install .

test_produce_local:
	go run . -url=redis://localhost:6379/0/abcd run echo hey

test_produce_local_exit_1:
	go run . -url=redis://localhost:6379/0/abcd run test_helpers/exit_1.sh

test_append_local:
	echo "test append" | go run . -url=redis://localhost:6379/0/abcd append

test_produce_large:
	go run . -url=redis://localhost:6379/0/abcd run test_helpers/large_echo.sh

test_consume_local:
	go run . -url=redis://localhost:6379/0/abcd follow

test_colors:
	go run . -url=redis://localhost:6379/0/abcd run test_helpers/color_output.sh

test_quotes:
	go run . -url=redis://localhost:6379/0/abcd run echo "this isn't okay"
