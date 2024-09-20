#!/bin/bash

set -u

REDIS=${REDIS:-redis://localhost/0}

function wrap {
	local stream errout
	errout=$(mktemp)
	stream="$1"
	shift

	go run ./ -url "$REDIS/$stream" -max-size-mbs=0.001 run "$@" 2>"$errout"

	code=$?

	sed >&2 -e 's/^/logstream stderr: /' $errout

	return "$code"
}

function follow {
	local stream
	stream="$1"

	go run ./ -url "$REDIS/$stream" follow
}

# usage: tester_function command...
function run_test {
	local tmp stream test_fn
	tmp=$(mktemp -d)
	stream=$(basename "$tmp")
	test_fn="$1"
	shift

	echo "Running test $test_fn on command $@  (tmpdir $tmp)"

	"$@" &>"$tmp/original"
	wrap "$stream" "$@" >"$tmp/runned"
	follow "$stream" >"$tmp/follow"

	"$test_fn" "$tmp"

	code=$?
	if [ "$code" -ne 0 ]; then
		echo "failed"
		exit 1
	else
		echo "ok"
	fi

	return $code
}

function diffsize {
	local size x y
	size="$1"
	x="$2"
	y="$3"

	diff <(head -c "$size" "$x") <(head -c "$size" "$y")
}

function both_equal {
	local dir
	dir="$1"

	diff "$dir/original" "$dir/runned" || {
		echo "output of 'logstream run' differs from original"
		return 1
	}
	diff "$dir/runned" "$dir/follow" || {
		echo "output of 'logstream follow' differs from original"
		return 1
	}
}

function exit_1 {
	local dir size
	dir="$1"

	diff "$dir/original" "$dir/runned" || {
		echo "runned data differs from original"
		return 1
	}

	size=$(wc -c "$dir/runned" | awk '{print $1}')
	diffsize "$size" "$dir/runned" "$dir/follow" || {
		echo "part of the output is lost"
		return 1
	}
	grep -q "exit status" "$dir/follow" || {
		echo "no command exit status message found in follow"
		return 1
	}
}

function output_limited {
	local dir
	dir="$1"

	diff "$dir/original" "$dir/runned" || {
		echo "runned data differs from original"
		return 1
	}
	diffsize 1000 "$dir/runned" "$dir/follow" || {
		echo "first 1000 bytes of runned and follow differs"
		return 1
	}
	grep -q "Logs exceeded limit" "$dir/follow" || {
		echo "no truncation warning found in follow"
		return 1
	}
}

if [ "$#" -eq 0 ]; then
	run_test both_equal echo one two three
	run_test both_equal ./test_helpers/color_output.sh
	run_test exit_1 ./test_helpers/exit_1.sh
	run_test output_limited ./test_helpers/large_echo.sh
else
	"$@"
fi
