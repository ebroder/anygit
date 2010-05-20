# Hack to get stunnel running, independent of platform.
# Ubuntu seems to package one version of the CLI, Fedora another.

if [ -L "$(which stunnel)" ]; then # heuristic for ubuntu
    stunnel -c -d 27017 -r 18.7.29.239:30000 -P /tmp/gdb_stunnel_pid
else
    stunnel "$(dirname "$0")/../conf/stunnel.conf"
fi