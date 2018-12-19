#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';
use Test::More;
no if $] >= 5.017011, warnings => 'experimental::smartmatch';    # Suppress smartmatch warnings

use JoinHero;

plan tests => 23;

diag("Testing DDL Parsing for JoinHero $JoinHero::VERSION, Perl $], $^X");

# Schema can be done one of two ways:
# 1. the normal dot notation (JJJ.STORES)
# 2. prefixed up to the first underscore (JJJ_Stores) with JJJ in the supportedMarts array ref
ok(JoinHero::getSchemaName('JJJ.STORES') eq 'JJJ');
ok(JoinHero::getSchemaName('JJJ_STORES', ['JJJ']) eq 'JJJ');
ok(!defined(JoinHero::getSchemaName('STORES')));
ok(!defined(JoinHero::getSchemaName('JJJ_STORES')));

# Table names might or might not have a schema prefix
ok(JoinHero::getTableName('JJJ.STORES') eq 'STORES');
ok(JoinHero::getTableName('STORES') eq 'STORES');

# Let's check breaking out some DDL components
my $s_sl_fk = q{ALTER TABLE JJJ.STORES
    ADD CONSTRAINT S_SL_FK FOREIGN KEY ( LOCATION_ID )
      REFERENCES JJJ.STORE_LOCATIONS ( LOCATION_ID );};

my $stores_locations_pk
  = qq{ALTER TABLE JJJ.STORE_LOCATIONS ADD CONSTRAINT STORES_LOCATIONS_PK PRIMARY KEY ( LOCATION_ID );};

# Parse DDL Source file into usable components
my ($pk_01, $fk_01) = JoinHero::getKeyComponents("$s_sl_fk \n $stores_locations_pk", []);

# Let's make sure this broke out like we expected
# FK components
ok($fk_01->{S_SL_FK}->{fromTable} eq 'STORES');
ok($fk_01->{S_SL_FK}->{toTable} eq 'STORE_LOCATIONS');
ok($fk_01->{S_SL_FK}->{fromSchema} eq 'JJJ');
ok($fk_01->{S_SL_FK}->{toSchema} eq 'JJJ');
ok($fk_01->{S_SL_FK}->{fromFields} ~~ ['LOCATION_ID']);
ok($fk_01->{S_SL_FK}->{toFields}   ~~ ['LOCATION_ID']);
ok($fk_01->{S_SL_FK}->{fromFieldList} eq 'LOCATION_ID');
ok($fk_01->{S_SL_FK}->{toFieldList} eq 'LOCATION_ID');

# PK components
ok($pk_01->{STORES_LOCATIONS_PK}->{pkName} eq 'STORES_LOCATIONS_PK');
ok($pk_01->{STORES_LOCATIONS_PK}->{table} eq 'STORE_LOCATIONS');
ok($pk_01->{STORES_LOCATIONS_PK}->{schema} eq 'JJJ');
ok($pk_01->{STORES_LOCATIONS_PK}->{pkType} eq 'PRIMARY KEY');
ok($pk_01->{STORES_LOCATIONS_PK}->{fieldList} eq 'LOCATION_ID');
ok($pk_01->{STORES_LOCATIONS_PK}->{fields} ~~ ['LOCATION_ID']);

# Just S_SL_FK by itself with no matching target pk/uk should result in a 'MANY' cardinality
my ($pk_02, $fk_02) = JoinHero::getKeyComponents("$s_sl_fk", []);
ok(JoinHero::getJoinCardinality($pk_02, $fk_02->{S_SL_FK}) eq 'MANY');

# S_SL_FK with a matching target pk/uk should result in a 'ONE' cardinality
my ($pk_03, $fk_03) = JoinHero::getKeyComponents("$s_sl_fk \n $stores_locations_pk");
ok(JoinHero::getJoinCardinality($pk_03, $fk_03->{S_SL_FK}) eq 'ONE');

# If the join order is flipped, we would expect a 'MANY' cardinality
ok(JoinHero::getJoinCardinality($pk_03, $fk_03->{S_SL_FK}, 'from') eq 'MANY');

