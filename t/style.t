#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';
use File::Spec;
use Test::More;
use English qw(-no_match_vars);

if (not $ENV{TEST_AUTHOR}) {
  my $msg = 'Author test.  Set $ENV{TEST_AUTHOR} to a true value to run.';
  plan(skip_all => $msg);
}

# Make sure we adhere to the supplied style guidelines
eval { require Test::Code::TidyAll; };
if ($EVAL_ERROR) {
  my $msg = 'Test::Code::TidyAll required to check code against style guide';
  plan(skip_all => $msg);
}

Test::Code::TidyAll::tidyall_ok();
