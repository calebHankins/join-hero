#!/usr/bin/perl -w
use strict;
use warnings;
use Test::More;

use JoinHero;

plan tests => 8;

testLogger();

sub testLogger {

  # Let's make sure logger has all the functions we expect and store values as expected
  my $died          = 0;
  my $checkErrCnt   = 0;
  my $checkFatalCnt = 0;

  # Debug / Info
  $JoinHero::logger->trace("trace");
  $JoinHero::logger->debug("debug");
  $JoinHero::logger->info("info");

  # Warning
  $JoinHero::logger->warn("warn");
  $JoinHero::logger->carp("carp");
  $JoinHero::logger->cluck("cluck");

  # Error
  $JoinHero::logger->error("error");
  $checkErrCnt = $JoinHero::logger->get_count("ERROR");
  ok($checkErrCnt == 1);

  # Error and die
  $died = 0;
  eval { $JoinHero::logger->error_die("error_die"); };
  $died = 1 if $@;
  ok($died);
  $checkErrCnt = $JoinHero::logger->get_count("ERROR");
  ok($checkErrCnt == 2);

  $died = 0;
  eval { $JoinHero::logger->croak("croak"); };
  $died = 1 if $@;
  ok($died);
  $checkErrCnt = $JoinHero::logger->get_count("ERROR");
  ok($checkErrCnt == 3);

  $died = 0;
  eval { $JoinHero::logger->confess("confess"); };
  $died = 1 if $@;
  ok($died);
  $checkErrCnt = $JoinHero::logger->get_count("ERROR");
  ok($checkErrCnt == 4);

  # Fatal
  $JoinHero::logger->fatal("fatal");
  $checkFatalCnt = $JoinHero::logger->get_count("FATAL");
  ok($checkFatalCnt == 1);

} ## end sub testLogger
