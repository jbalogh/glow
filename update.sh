#!/bin/sh

git fetch -q && git checkout -q origin/master && ./glow.sh restart > /dev/null
