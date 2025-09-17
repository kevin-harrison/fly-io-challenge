#!/bin/bash

set -eu

GO_MODULE_BIN="$HOME/go/bin/maelstrom-unique-ids"
MAELSTROM_BIN="../maelstrom/maelstrom"
RUN_TIME=5

if [ ! -f $MAELSTROM_BIN ]; then
	echo "Maelstrom executable does not exist in ${MAELSTROM_BIN}."
	exit 1
fi

run() {
	$MAELSTROM_BIN test \
		-w unique-ids \
		--bin $GO_MODULE_BIN \
		--time-limit $RUN_TIME \
		--rate 1000 \
		--node-count 3 \
		--availability total \
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
