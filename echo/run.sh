#!/bin/bash

set -eu

GO_MODULE_BIN="$HOME/go/bin/maelstrom-echo"
MAELSTROM_BIN="../maelstrom/maelstrom"
RUN_TIME=5

if [ ! -f $MAELSTROM_BIN ]; then
	echo "Maelstrom executable does not exist in ${MAELSTROM_BIN}."
	exit 1
fi

run() {
	$MAELSTROM_BIN test \
		-w echo \
		--bin $GO_MODULE_BIN \
		--node-count 1 \
		--time-limit $RUN_TIME
}


build () {
	go install .
}

main() {
	build
	run
}

main "$@"
