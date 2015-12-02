#!/usr/bin/perl
  #use strict;
  #use warnings;

  my $myfile = $ARGV[0];
  my $pos = $ARGV[1];
  my $length = $ARGV[2];
  open FILE, $myfile or die "Couldn't open file: $!"; 
  while (<FILE>){
	  $string .= $_;
  }
  close FILE;
  
  my $fragment =  substr $string, $pos, $length;
  #print "  string: <$string>\n";
  print "fragment: <$fragment>\n";

