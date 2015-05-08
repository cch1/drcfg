#!/bin/bash

usage ()
{
cat << EOF
Usage: $(basename $0) USER@HOST | kill | status

GENERAL OPTIONS:
 USER@HOST:
 - USER is your ssh jump host user.
 - HOST is the roomkey bastion/jump host.

kill:
 Option will kill dev tunnels running.

status:
 Option will detect if tunnel is in use.

EOF
}

check_tunnel ()
{
	if ps -ef | grep -q -e "[2]181:.*:2181.*9900:.*:80"; then
		echo "Detected tunnel in use."
		return 0
	else
		echo "No tunnel detected."
		return 1
	fi
}


if [ -z "$1" ]; then
	usage
	exit 1
fi

if [ "$1" == "status" ]; then
	if check_tunnel; then
		exit 0
	else
		exit 1
	fi
fi

if check_tunnel; then
    echo "Detected tunnel in use; killing old tunnels."
    ps -ef | grep -e "[2]181:.*:2181.*9900:.*:80" | awk '{print $2}' | xargs kill -9
fi
if [ "$1" == "kill" ]; then
	echo Done.
	exit 0
fi

ssh -f -N -o ExitOnForwardFailure=yes \
	-L 2181:zk-dev:2181 \
	-L 9900:profjv2-dev:80 \
	"$@"
echo Done.
