# CondorSnakey

Scripts to reboot a HTCondor system without downtime

How to use:

./snakey.pl will drain the nodes and boot them.

./post_snakey.pl will wait until the nodes are good, then put them online.

Both scripts need to be running. This is how to set them up.

On the HTCondor head node, make a directory /root/scripts, and install the snakey rpm.

Open two screen on the system.

Use ssh-agent bash, ssh-add in both screens, or similar to get passwordless access from HTCondor headnode to workernodes.

In both screens, cd /root/scripts/snakey

In one screen, rm /root/scripts/testnodes-exemptions.txt; touch /root/scripts/testnodes-exemptions.txt (i.e. make an empty file)

In screen 1, make a file with the names of all the workernodes in it, called /root/scripts/snakey/nodesToBoot.txt

Then cd /root/scripts/snakey; ./snakey.pl -n nodesToBoot.txt -s 25

In screen 2, cd /root/scripts/snakey; ./post_snakey.pl -t /usr/libexec/HTCondor/scripts/testnode.sh

Note: testnode.sh is some command that returns 0 when the node is good. You need to write your own, or just use /bin/true

sj, Tue Feb 28 11:41:47 GMT 2017

