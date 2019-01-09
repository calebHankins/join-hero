#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';
use Test::More;
no if $] >= 5.017011, warnings => 'experimental::smartmatch';    # Suppress smartmatch warnings

use JoinHero;

plan tests => 7;

checkRequiredParm();
signOff();
getUniqArray();
createExportFile();

sub getUniqArray {
  my @uniqLUE = JoinHero::getUniqArray((42, 42, 42, 42, 42));
  my @oneLUE  = (42);
  is_deeply(@oneLUE, @uniqLUE);

  return;
} ## end sub getUniqArray

sub checkRequiredParm {
  my $checkRequiredParmErrCnt = 0;

  # Populated
  my $populatedValue = 42;
  JoinHero::checkRequiredParm($populatedValue);
  $checkRequiredParmErrCnt = $JoinHero::logger->get_count("ERROR");
  ok($checkRequiredParmErrCnt == 0);

  # Unpopulated
  my $unpopulatedValue = '';
  JoinHero::checkRequiredParm($unpopulatedValue);
  $checkRequiredParmErrCnt = $JoinHero::logger->get_count("ERROR");
  ok($checkRequiredParmErrCnt == 1);

  # Undefined
  my $undefinedValue;
  JoinHero::checkRequiredParm($undefinedValue);
  $checkRequiredParmErrCnt = $JoinHero::logger->get_count("ERROR");
  ok($checkRequiredParmErrCnt == 2);

  return;
} ## end sub checkRequiredParm

sub signOff {
  ok(JoinHero::signOff(42) == 42);
  ok(JoinHero::signOff(256) == 1);

  return;
} ## end sub signOff

sub createExportFile {
  my $testWritePath     = 'scratch/write-test.txt';
  my $testWriteContents = 'testing createExportFile';
  JoinHero::createExportFile($testWriteContents, $testWritePath);
  my $writeStatus = (-f $testWritePath) ? 1 : 0;
  if ($writeStatus) { unlink $testWritePath or $JoinHero::logger->warn("Could not unlink file [$testWritePath]"); }
  ok($writeStatus);

  return;
} ## end sub createExportFile
