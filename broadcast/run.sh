#!/bin/bash

set -eu

GO_MODULE_BIN="$HOME/go/bin/maelstrom-broadcast"
MAELSTROM_BIN="../maelstrom/maelstrom"
RUN_TIME=5

if [ ! -f $MAELSTROM_BIN ]; then
	echo "Maelstrom executable does not exist in ${MAELSTROM_BIN}."
	exit 1
fi

usage() {
  echo "usage: ${0##*/} <a|b|c|d>" >&2
}

part_a() {
	$MAELSTROM_BIN test \
		-w broadcast \
		--bin $GO_MODULE_BIN \
		--node-count 1 \
		--time-limit $RUN_TIME \
		--rate 10
}

part_b () {
	$MAELSTROM_BIN test \
		-w broadcast \
		--bin $GO_MODULE_BIN \
		--node-count 5 \
		--time-limit $RUN_TIME \
		--rate 10
}

part_c () {
	$MAELSTROM_BIN test \
		-w broadcast \
		--bin $GO_MODULE_BIN \
		--node-count 5 \
		--time-limit $RUN_TIME \
		--rate 10 \
		--nemesis partition
}


part_d () {
	$MAELSTROM_BIN test \
		-w broadcast \
		--bin $GO_MODULE_BIN \
		--node-count 25 \
		--time-limit $RUN_TIME \
		--rate 100 \
		--latency 100
}

build () {
	go install .
}

main() {
	case "${1-}" in
		a) build; part_a ;;
		b) build; part_b ;;
		c) build; part_c ;;
		d) build; part_d ;;
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
