#! /bin/sh
ROOT=/home/jbalogh/glow
PIDFILE=$ROOT/pid
MANAGE=/home/jbalogh/glow/manage.py

. /etc/init.d/functions

start() {
    echo "Starting glow..."
    if [ -e $PIDFILE ] && [ -e /proc/$(cat $PIDFILE) ]; then
        echo "glow is already running" $(cat $PIDFILE)
        return 1
    fi
    python26 $MANAGE glow &
    RETURN=$?
    echo $! > $PIDFILE
    return $RETURN
}

stop() {
    echo "Stopping glow..."
    if [ -e $PIDFILE ] && [ -e /proc/$(cat $PIDFILE) ]; then
        killproc -p $PIDFILE
        return $?
    else
        echo "glow is not running"
        return 1
    fi
}

case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  restart)
        stop
        start
        ;;
  *)
        echo $"Usage: $0 {start|stop|restart}"
        exit 1
esac
