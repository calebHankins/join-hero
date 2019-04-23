#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';
use Test::More;
use English qw(-no_match_vars);
no if $] >= 5.017011, warnings => 'experimental::smartmatch';    # Suppress smartmatch warnings

use JoinHero;

# $JoinHero::verbose = 1;    # Uncomment this and run tests with --verbose to ease debugging

plan tests => 10;

# Skip these tests if we don't have the graph library
eval { require Graph; };
if ($EVAL_ERROR) {
  my $msg = 'Graph required to check graph based functionality';
  plan(skip_all => $msg);
}

getGraph();

sub getGraph {
  my $subName = (caller(0))[3];

  # Let's create some testing DDL and make sure it gets translated as we expect
  my $s_sl_fk = q{ALTER TABLE JJJ.STORES
    ADD CONSTRAINT S_SL_FK FOREIGN KEY ( LOCATION_ID )
      REFERENCES JJJ.STORE_LOCATIONS ( LOCATION_ID );};

  my $stores_locations_pk
    = qq{ALTER TABLE JJJ.STORE_LOCATIONS ADD CONSTRAINT STORES_LOCATIONS_PK PRIMARY KEY ( LOCATION_ID );};

  # Parse DDL Source file into usable components
  my ($pk_01, $fk_01) = JoinHero::getKeyComponents("$s_sl_fk \n $stores_locations_pk");

  # Build graph from fkComponents
  my $g = JoinHero::getGraph({fkComponents => $fk_01});

  # Check edges
  my @ue = $g->unique_edges;
  $JoinHero::logger->info("$subName \@ue: " . JoinHero::Dumper(@ue));
  my $expectedEdge1 = ['STORE_LOCATIONS', 'STORES'];
  my $expectedEdge2 = ['STORES',          'STORE_LOCATIONS'];
  ok($expectedEdge1 ~~ @ue, 'Expected edge 1 found');
  ok($expectedEdge2 ~~ @ue, 'Expected edge 2 found');

  # Check vertices
  my @uv = $g->unique_vertices;
  $JoinHero::logger->info("$subName \@uv: " . JoinHero::Dumper(@uv));
  my $expectedVertex1 = 'STORE_LOCATIONS';
  my $expectedVertex2 = 'STORES';
  ok($expectedVertex1 ~~ @uv, 'Expected vertex 1 found');
  ok($expectedVertex2 ~~ @uv, 'Expected vertex 2 found');

  # Check edge weights
  my $ew1to2 = $g->get_edge_weight($expectedVertex1, $expectedVertex2);
  $JoinHero::logger->info("$subName \$ew1to2: " . JoinHero::Dumper($ew1to2));
  my $ew2to1 = $g->get_edge_weight($expectedVertex2, $expectedVertex1);
  $JoinHero::logger->info("$subName \$ew2to1: " . JoinHero::Dumper($ew2to1));
  ok($ew1to2 == 1,  "Expected weight for 'ONE' cardinality join found");
  ok($ew2to1 == 10, "Expected weight for 'MANY' cardinality join found");

  # Check attributes
  my $ea1to2 = $g->get_edge_attributes($expectedVertex1, $expectedVertex2);
  $JoinHero::logger->info("$subName \$ea1to2: " . JoinHero::Dumper($ea1to2));
  ok($ea1to2->{fkKey} eq 'S_SL_FK-_-JJJ.STORES-_-JJJ.STORE_LOCATIONS', "ea1to2 fkKey attribute matches expected value");
  ok($ea1to2->{direction} eq 'NORMAL', "ea1to2 direction attribute matches expected value");
  my $ea2to1 = $g->get_edge_attributes($expectedVertex2, $expectedVertex1);
  $JoinHero::logger->info("$subName \$ea2to1: " . JoinHero::Dumper($ea2to1));
  ok($ea2to1->{fkKey} eq 'S_SL_FK-_-JJJ.STORES-_-JJJ.STORE_LOCATIONS', "ea2to1 fkKey attribute matches expected value");
  ok($ea2to1->{direction} eq 'REVERSED', "ea2to1 direction attribute matches expected value");

  return;
} ## end sub getGraph
