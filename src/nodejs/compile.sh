#!/bin/bash

echo "Make sure /home/jaguar/jaguar/lib has .so and jaguarnode.node files"

node-gyp configure clean
node-gyp configure build

if [[ -f "build/Release/jaguarnode.node" ]]; then
	/bin/cp -f build/Release/jaguarnode.node .
else
    echo "build/Release/jaguarnode.node not found"
fi
