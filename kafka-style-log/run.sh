#!/bin/bash

set -eu

GO_MODULE_BIN="$HOME/go/bin/maelstrom-kafka-style-log"
MAELSTROM_BIN="../maelstrom/maelstrom"
RUN_TIME=5

if [ ! -f $MAELSTROM_BIN ]; then
	echo "Maelstrom executable does not exist in ${MAELSTROM_BIN}."
	exit 1
fi

usage() {
  echo "usage: ${0##*/} <a|b|c>" >&2
}

part_a() {
	# Will fail: Code is hardcoded to support node-count=2
	$MAELSTROM_BIN test \
		-w kafka \
		--bin $GO_MODULE_BIN \
		--node-count 1 \
		--concurrency 2n \
		--time-limit $RUN_TIME \
		--rate 1000
}

part_b () {
	$MAELSTROM_BIN test \
		-w kafka \
		--bin $GO_MODULE_BIN \
		--node-count 2 \
		--concurrency 2n \
		--time-limit $RUN_TIME \
		--rate 1000
}

part_c () {
	part_b
}

build () {
	go install .
}

main() {
	case "${1-}" in
		a) build; part_a ;;
		b) build; part_b ;;
		c) build; part_c ;;
		""|-h|--help)
			usage
			exit 1 ;;
		*)
			echo "invalid part arg" >&2
			usage
			exit 1 ;;
	esac
}

main "$@"
