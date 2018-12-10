#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

# Make sure we could load the module
BEGIN {
  use_ok('JoinHero') || print "Could not load module\n";
}

diag("Testing JoinHero $JoinHero::VERSION, Perl $], $^X");

