#!/bin/bash

set -eu

GO_MODULE_BIN="$HOME/go/bin/maelstrom-grow-only-counter"
MAELSTROM_BIN="../maelstrom/maelstrom"
RUN_TIME=5

if [ ! -f $MAELSTROM_BIN ]; then
	echo "Maelstrom executable does not exist in ${MAELSTROM_BIN}."
	exit 1
fi

run() {
	$MAELSTROM_BIN test \
		-w g-counter \
		--bin $GO_MODULE_BIN  \
		--node-count 3 \
		--rate 100 \
		--time-limit $RUN_TIME \
		--nemesis partition
}


build () {
	go install .
}

main() {
	build
	run
}

main "$@"
