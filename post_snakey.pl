#!/usr/bin/perl

use strict;
use Fcntl ':flock'; 
use Getopt::Long;

sub initParams();

my %parameter;

initParams();

my %offlineTimes;

while ( 1 ) {
  %offlineTimes = getOfflineTimes();
  my @a=keys(%offlineTimes);
  my $count = $#a;

  if ($count == -1 ) {
    print("No work to do\n");
    exit(0);
  }
  
  foreach my $n (keys(%offlineTimes)) {
  
    my $uptime = -1;
    open(B,"ssh -o ConnectTimeout=2 -o BatchMode=yes $n cat /proc/uptime 2>&1|");
    while(<B>) {
      if (/([0-9\.]+)\s+[0-9\.]+/) {
        $uptime = $1;
      }
    }
    close(B);
    if ($uptime == -1) {
      print("Refusing to remove $n because it may not have been rebooted\n");
    }
    else {
      my $offlineTime = $offlineTimes{$n};
      my $timeNow = time();
      if ($timeNow - $uptime <= $offlineTime ) {
        print("Refusing to remove $n. ");
        printf("Last reboot - %6.3f  days ago. ", $uptime / 24 / 60 /60);
        printf("Offlined    - %6.3f  days ago.\n", ($timeNow - $offlineTime)  / 24 / 60 /60);
      }
      else {
        print("$n has been rebooted\n");
        open(B,"ssh -o ConnectTimeout=2 -o BatchMode=yes $n $parameter{'TESTCOMMAND'}|");
        while(<B>) { }
        close(B);
        my $status = $? >> 8;
        if ($status == 0) {
          print("$n passes test command; will remove from status file and put online\n");
          removeFromExemptions($n); 
          putOnline           ($n); 
        }
        else {
          print("$n is not passing testnode.sh - $status\n");
        }
      }
    }
  }
  sleep 567;
}

#-----------------------------------------
sub putOffline() {

  my $node = shift();

  my @result1 = runCommand("condor_config_val -verbose -startd -set \"StartJobs = false\" -name $node");
  my @result2 = runCommand("condor_reconfig $node");
  my @result3 = runCommand("condor_reconfig -daemon startd $node");
  if (($result1[0] == 0) or ($result2[0] == 0) or ($result3[0] == 0)) {
    print("Put offline() for $node may have failed.\n");
  }
  if ($result1[1] !~ /Successfully set configuration/m) {
    print("Put offline for $node may have failed\n");
  }
}
#-----------------------------------------
sub putOnline() {

  my $node = shift();

  my @result1 = runCommand("condor_config_val -verbose -startd -set \"StartJobs = true\" -name $node");
  my @result2 = runCommand("condor_reconfig $node");
  my @result3 = runCommand("condor_reconfig -daemon startd $node");
  if (($result1[0] == 0) or ($result2[0] == 0) or ($result3[0] == 0)) {
    print("Put online() for $node may have failed.\n");
  }
  if ($result1[1] !~ /Successfully set configuration/m) {
    print("Put online for $node may have failed\n");
  }
}
#-----------------------------------------
sub runCommand() {

  my $cmd = shift();
  my $output = '';

  my $status = open(CMD,"$cmd|");
  if (! $status) {
    return [0,''];
  }
  while (<CMD>) {
    $output = $output . $_;
  }
  close(CMD);
  my @result;
  $result[0] = 1;
  $result[1] = $output;
  return @result;
}
#-----------------------------------------
sub getOfflineTimes() {
  my %offlineTimes = ();
  open(TN,"<$parameter{'STATUSFILE'}") or die("Could not open status file $parameter{'STATUSFILE'}, $!\n");
  while(<TN>) {
    if (/(\S+)\s+\# snakey.pl put this offline (\d+)/) {
      $offlineTimes{$1} = $2;
    }
  }
  close(TN);
  return %offlineTimes;
}

#-----------------------------------------
sub removeFromExemptions($) {

  my $node = shift();

  open(TN,"<$parameter{'STATUSFILE'}") or die("Could not open status file $parameter{'STATUSFILE'}, $!\n");
  my @lines = <TN>; 
  close( TN ); 
  open(TN,">$parameter{'STATUSFILE'}") or die("Could not open to write status file $parameter{'STATUSFILE'}, $!\n");
  flock(TN, LOCK_EX) or die "Could not lock $parameter{'STATUSFILE'}, $!";
  foreach my $line ( @lines ) { 
    print TN $line unless ( $line =~ m/$node/ ); 
  } 
  close(TN) or die "Could not write $parameter{'STATUSFILE'}, $!";
}


#-----------------------------------------
sub initParams() {

  GetOptions ('h|help'       =>   \$parameter{'HELP'},
              't:s'          =>   \$parameter{'TESTCOMMAND'} ,
              's:s'          =>   \$parameter{'STATUSFILE'} ,
              );

  if (defined($parameter{'HELP'})) {
    print <<TEXT;

Abstract: A tool to drain and boot a bunch of nodes

  -h  --help           Prints this help page
  -t          somecmd  Test command to run before onlining node (should return zero)
  -s          status   File where snakey.pl writes its status info

TEXT
    exit(0);
  }

  if (!defined($parameter{'TESTCOMMAND'})) {
    $parameter{'TESTCOMMAND'} = '/bin/sleep 300';
  }
  if (!defined($parameter{'STATUSFILE'})) {
    $parameter{'STATUSFILE'} = '/root/scripts/testnodes-exemptions.txt';
  }

}
#-----------------------------------------


