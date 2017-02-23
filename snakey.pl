#!/usr/bin/perl

use strict;
use Fcntl ':flock'; 
use Getopt::Long;

sub initParams();

my %parameter;

initParams();

my @nodesToDo;

open(NODES,"$parameter{'NODES'}") or die("Cannot open file of nodes to reboot, $!\n");
while(<NODES>) {
  chomp($_);
  push(@nodesToDo,$_); 
}
close(NODES);

checkOk(@nodesToDo);

my @selection = selectSome($parameter{'SLICE'}); 
foreach my $n(@selection) { 
  print "Putting $n offline\n"; 
  finish($n);
}

while( $#selection > -1) {

  my $drainedNode = '';
  while($drainedNode eq '') {
    sleep( 600 );
    $drainedNode = checkIfOneHasDrained(@selection);
  }
  
  @selection = remove($drainedNode,@selection);

  print("Rebooting $drainedNode\n");
  my $status = rebootNode($drainedNode);
  print("status -- $status\n");

  my @nextOne = selectSome(1);
  if ($#nextOne == 0) {
    my $nextOne = $nextOne[0];
    print "Putting $nextOne offline\n"; 
    finish($nextOne);
    push(@selection,$nextOne);
  }
}
#-----------------------------------------
sub finish() {
  my $node = shift();

  putOffline($node);

  system("touch /root/scripts/testnodes-exemptions.txt");
  open(TN,"/root/scripts/testnodes-exemptions.txt") or die("Could not open testnodes.exemptions.txt, $!\n");
  while(<TN>) {
    my $l = $_;
    chomp($l);
    $l =~ s/#.*//;
    $l =~ s/\s*//g;
    if ($node =~ /^$l$/) {
      print ("Node $node is already in testnodes-exemptions.txt\n");
      return;
    }
  }
  close(TN);
  open(TN,">>/root/scripts/testnodes-exemptions.txt") or die("Could not open testnodes.exemptions.txt, $!\n");
  flock(TN, LOCK_EX) or die "Could not lock /root/scripts/testnodes-exemptions.txt, $!";
  print (TN "$node # snakey.pl put this offline " . time() . "\n");
  close(TN) or die "Could not write /root/scripts/testnodes-exemptions.txt, $!";
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
sub runCommand($) {

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
sub remove() {
  my $drained = shift();
  my @poolOfNodes = @_;

  my @newSelection = ();
  foreach my $n (@poolOfNodes) {
    if ($n !~ /$drained/) {
      push(@newSelection,$n);
    }
  }
  die("None removed\n") unless($#newSelection == ($#poolOfNodes -1));
  return @newSelection;
}

#-----------------------------------------
sub checkIfOneHasDrained(@) {
  my @nodesToCheck = @_; 
  foreach my $n (@nodesToCheck) {
    my $hadReport = 0;
    my $state = "notoffline";
    my $jobCount = 0;

    open(CONDORSTATUS,"condor_status -long $n|");
    while(<CONDORSTATUS>) {
      $hadReport = 1;
      my $l = $_;
      chomp($l);
      if ($l =~ /^JobId /) {
        $jobCount = $jobCount + 1;
      }
      if ($l =~ /^StartJobs \= false/) {
        $state = 'offline';
      }
    }
    close(CONDORSTATUS);
    
    print("Result of check on $n: hadReport - $hadReport, state - $state, jobCount - $jobCount\n");
    if (($hadReport) && ($state eq 'offline') &&($jobCount ==0)) {
      return $n;
    }
  }
  return "";
}

#-----------------------------------------
sub selectSome($) {
  my $max = shift;
  my @some = (); 
  for (my $ii = 0; $ii < $max; $ii++) {
    if (defined($nodesToDo[0])) {
      push(@some,shift(@nodesToDo));
    } 
  }
  return @some;
}

#-----------------------------------------
sub checkOk(){
  my @nodes = @_;
  
  foreach my $n (@nodes) {

    my @result = runCommand("condor_config_val -verbose -name $n -startd StartJobs");
    if ($result[0] == 0) {
      die ("Cannot check node $n.\n");
    }

    my $state = undef;
    my $resultMessage = $result[1];
    $resultMessage =~ /^STARTJOBS = (.*)/m;
    if (! defined($1) ) {
      die ("Cannot check node $n\n");
    }
    $state = $1;
    if ($state =~ /false/i) {
      die ("Node $n was already offline\n");
    }
  }
  return;
}

#-----------------------------------------
sub initParams() {

  GetOptions ('h|help'       =>   \$parameter{'HELP'},
              'n:s'          =>   \$parameter{'NODES'} ,
              's:i'          =>   \$parameter{'SLICE'} ,
              );

  if (defined($parameter{'HELP'})) {
    print <<TEXT;

Abstract: A tool to drain and boot a bunch of nodes

  -h  --help                  Prints this help page
  -n                 nodes    File of nodes to boot
  -s                 slice    Size of slice to offline at once

TEXT
    exit(0);
  }

  if (!defined($parameter{'SLICE'})) {
    $parameter{'SLICE'} = 5;
  }

  if (!defined($parameter{'NODES'})) {
    die("Please give a file of nodes to reboot\n");
  }

  if (! -s  $parameter{'NODES'} ) {
    die("Please give a real file of nodes to reboot\n");
  }
}
#-----------------------------------------
sub rebootNode($) {
  my $nodeToBoot = shift();
  my $nodeToCheck = $nodeToBoot;
  my $condorstatusWorked = 0;
  my $hasJobs        = 0;

  open(CONDORSTATUS,"condor_status -long $nodeToCheck|");
  while(<CONDORSTATUS>) {
    $condorstatusWorked = 1;
    my $l = $_;
    chomp($l);
    if ($l =~ /^JobId /) {
      $hasJobs = $hasJobs + 1;
    }
  }
  close(CONDORSTATUS);

  if (! $condorstatusWorked) { return 0; }
  if (  $hasJobs       ) { return 0; }

  open(REBOOT,"ssh -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=10 $nodeToBoot reboot|");
  while(<REBOOT>) {
    print;
  }
  return 1;
}

#-----------------------------------------
