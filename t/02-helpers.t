#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';
use Test::More;
no if $] >= 5.017011, warnings => 'experimental::smartmatch';    # Suppress smartmatch warnings

use JoinHero;

plan tests => 9;

checkRequiredParm();
signOff();
getUniqArray();
createExportFile();

sub getUniqArray {

  # 1d array
  my @uniqLUE = JoinHero::getUniqArray((42, 42, 42, 42, 42));
  my @oneLUE = (42);
  is_deeply(\@oneLUE, \@uniqLUE, '1d uniq array test');

  # 2d array
  my @raw2dLUE = (
                  [42, 'Life, the Universe and Everything'],
                  [42, 'Life, the Universe and Everything'],
                  [42, 'Life, the Universe and Everything'],
                  [42, 'Life, the Universe and Everything'],
                  [42, 'Life, the Universe and Everything']
  );
  my @uniq2dLUE = JoinHero::getUniqArray(@raw2dLUE);
  my @one2dLUE = ([42, 'Life, the Universe and Everything']);
  is_deeply(\@one2dLUE, \@uniq2dLUE, '2d uniq array test');

  # 3d array
  my @raw3dLUE = (
                  [42, 'Life, the Universe and Everything', ['My', 'Lord', 'is',   'that', 'legal?']],
                  [42, 'Life, the Universe and Everything', ['My', 'Lord', 'is',   'that', 'legal?']],
                  [42, 'Life, the Universe and Everything', ['My', 'Lord', 'is',   'that', 'legal?']],
                  [42, 'Life, the Universe and Everything', ['My', 'Lord', 'is',   'that', 'legal?']],
                  [42, 'Life, the Universe and Everything', ['I',  'will', 'make', 'it',   'legal']]
  );
  my @uniq3dLUE = JoinHero::getUniqArray(@raw3dLUE);
  my @one3dLUE = (
                  [42, 'Life, the Universe and Everything', ['My', 'Lord', 'is',   'that', 'legal?']],
                  [42, 'Life, the Universe and Everything', ['I',  'will', 'make', 'it',   'legal']]
  );
  is_deeply(\@one3dLUE, \@uniq3dLUE, '3d uniq array test');

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
