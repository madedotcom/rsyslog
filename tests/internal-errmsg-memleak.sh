#!/bin/bash
# This tests a memory leak we have seen when processing internal error
# message with the settings used in this test. We use imfile as it is
# easist to reproduce this way. Note that we are only interested in
# whether or not we have a leak, not any other functionality. Most
# importantly, we do not care if the error message appears or not. This
# is because it is not so easy to pick it up from the system log and other
# tests already cover this szenario.
# add 2017-05-10 by Rainer Gerhards, released under ASL 2.0
uname
if [ `uname` = "SunOS" ] ; then # TODO: do we really need this test?
   echo "Solaris: inotify isn't supported on Solaris"
   exit 77
fi

. $srcdir/diag.sh init
. $srcdir/diag.sh generate-conf
. $srcdir/diag.sh add-conf '
global(processInternalMessages="off")
$RepeatedMsgReduction on # keep this on because many distros have set it

module(load="../plugins/imfile/.libs/imfile") # mode="polling" pollingInterval="1")
input(type="imfile" File="./rsyslog.input" Tag="tag1" ruleset="ruleset1")

template(name="tmpl1" type="string" string="%msg%\n")
ruleset(name="ruleset1") {
	action(type="omfile" file="rsyslog.out.log" template="tmpl1")
}
action(type="omfile" file="rsyslog2.out.log")
'
. $srcdir/diag.sh startup-vg-waitpid-only
./msleep 500 # wait a bit so that the error message can be emitted
. $srcdir/diag.sh shutdown-immediate
. $srcdir/diag.sh wait-shutdown-vg

. $srcdir/diag.sh exit
