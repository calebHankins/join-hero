#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';
use Test::More;
no if $] >= 5.017011, warnings => 'experimental::smartmatch';    # Suppress smartmatch warnings

use JoinHero;

# $JoinHero::verbose = 1;    # Uncomment this and run tests with --verbose to ease debugging

plan tests => 34;

diag("Testing DDL Parsing for JoinHero $JoinHero::VERSION, Perl $], $^X");

# Let's create some testing DDL and make sure it gets translated as we expect
my $s_sl_fk = q{ALTER TABLE JJJ.STORES
    ADD CONSTRAINT S_SL_FK FOREIGN KEY ( LOCATION_ID )
      REFERENCES JJJ.STORE_LOCATIONS ( LOCATION_ID );};

my $stores_locations_pk
  = qq{ALTER TABLE JJJ.STORE_LOCATIONS ADD CONSTRAINT STORES_LOCATIONS_PK PRIMARY KEY ( LOCATION_ID );};

# Parse DDL Source file into usable components
my ($pk_01, $fk_01) = JoinHero::getKeyComponents("$s_sl_fk \n $stores_locations_pk");

getSchemaName();
getTableName();
fkBreakOut();
pkBreakOut();
getJoinCardinality();
sqlGenerationS_SL_FKBasic();
getJoinFromComponents();

sub getSchemaName {

  # Schema can be done one of two ways:
  # 1. the normal dot notation (JJJ.STORES)
  # 2. prefixed up to the first underscore (JJJ_Stores) with JJJ in the supportedMarts array ref
  ok(JoinHero::getSchemaName('JJJ.STORES') eq 'JJJ');
  ok(JoinHero::getSchemaName('JJJ_STORES', ['JJJ']) eq 'JJJ');
  ok(!defined(JoinHero::getSchemaName('STORES')));
  ok(!defined(JoinHero::getSchemaName('JJJ_STORES')));
  return;
} ## end sub getSchemaName

sub getTableName {

  # Table names might or might not have a schema prefix
  ok(JoinHero::getTableName('JJJ.STORES') eq 'STORES');
  ok(JoinHero::getTableName('STORES') eq 'STORES');
} ## end sub getTableName

sub fkBreakOut {

  # Let's make sure this FK broke out like we expected
  my $fkKey = 'S_SL_FK-_-JJJ.STORES-_-JJJ.STORE_LOCATIONS';
  ok($fk_01->{$fkKey}->{fromTable} eq 'STORES');
  ok($fk_01->{$fkKey}->{toTable} eq 'STORE_LOCATIONS');
  ok($fk_01->{$fkKey}->{fromSchema} eq 'JJJ');
  ok($fk_01->{$fkKey}->{toSchema} eq 'JJJ');
  ok($fk_01->{$fkKey}->{fromFields} ~~ ['LOCATION_ID']);
  ok($fk_01->{$fkKey}->{toFields}   ~~ ['LOCATION_ID']);
  ok($fk_01->{$fkKey}->{fromFieldList} eq 'LOCATION_ID');
  ok($fk_01->{$fkKey}->{toFieldList} eq 'LOCATION_ID');
  ok($fk_01->{$fkKey}->{fkName} eq 'S_SL_FK');

  return;
} ## end sub fkBreakOut

sub pkBreakOut {

  # Let's make sure this PK broke out like we expected
  ok($pk_01->{STORES_LOCATIONS_PK}->{pkName} eq 'STORES_LOCATIONS_PK');
  ok($pk_01->{STORES_LOCATIONS_PK}->{table} eq 'STORE_LOCATIONS');
  ok($pk_01->{STORES_LOCATIONS_PK}->{schema} eq 'JJJ');
  ok($pk_01->{STORES_LOCATIONS_PK}->{pkType} eq 'PRIMARY KEY');
  ok($pk_01->{STORES_LOCATIONS_PK}->{fieldList} eq 'LOCATION_ID');
  ok($pk_01->{STORES_LOCATIONS_PK}->{fields} ~~ ['LOCATION_ID']);

  return;
} ## end sub pkBreakOut

sub getJoinCardinality {

  # Just S_SL_FK by itself with no matching target pk/uk should result in a 'MANY' cardinality
  my $fkKey = 'S_SL_FK-_-JJJ.STORES-_-JJJ.STORE_LOCATIONS';
  my ($pk_02, $fk_02) = JoinHero::getKeyComponents("$s_sl_fk");
  ok(JoinHero::getJoinCardinality($pk_02, $fk_02->{$fkKey}) eq 'MANY');

  # S_SL_FK with a matching target pk/uk should result in a 'ONE' cardinality
  my ($pk_03, $fk_03) = JoinHero::getKeyComponents("$s_sl_fk \n $stores_locations_pk");
  ok(JoinHero::getJoinCardinality($pk_03, $fk_03->{$fkKey}) eq 'ONE');

  # If the join order is flipped, we would expect a 'MANY' cardinality
  ok(JoinHero::getJoinCardinality($pk_03, $fk_03->{$fkKey}, 'from') eq 'MANY');
  ok(JoinHero::getJoinCardinality($pk_03, $fk_03->{$fkKey}, 'REVERSED') eq 'MANY');

  # Make sure we get something back if we supply a nonsense direction
  ok(JoinHero::getJoinCardinality($pk_03, $fk_03->{$fkKey}, 'Sideways') eq 'INVALID_DIRECTION');

  return;
} ## end sub getJoinCardinality

sub getJoinFromComponents {
  my $subName = (caller(0))[3];

  my $table1 = 'STORE_LOCATIONS';
  my $table2 = 'STORES';
  my $getJoinFromComponentsParamsNormal
    = {toTable => $table1, fromTable => $table2, pkComponents => $pk_01, fkComponents => $fk_01};

  my ($joinNormal) = JoinHero::getJoinFromComponents($getJoinFromComponentsParamsNormal);
  ok($joinNormal->{join}->{toTable} eq 'STORE_LOCATIONS', "$subName toTable NORMAL direction lookup");
  ok($joinNormal->{join}->{fromTable} eq 'STORES',        "$subName fromTable NORMAL direction lookup");
  ok($joinNormal->{direction} eq 'NORMAL',                "$subName direction NORMAL direction lookup");

  my $getJoinFromComponentsParamsReversed
    = {toTable => $table2, fromTable => $table1, pkComponents => $pk_01, fkComponents => $fk_01};

  my ($joinReversed) = JoinHero::getJoinFromComponents($getJoinFromComponentsParamsReversed);
  ok($joinReversed->{join}->{toTable} eq 'STORE_LOCATIONS', "$subName toTable REVERSE direction lookup");
  ok($joinReversed->{join}->{fromTable} eq 'STORES',        "$subName fromTable REVERSE direction lookup");
  ok($joinReversed->{direction} eq 'REVERSED',              "$subName direction REVERSED direction lookup");

} ## end sub getJoinFromComponents

sub sqlGenerationS_SL_FKBasic {

  # We're expecting to get back something like this:
  my $getJoinSQL_S_SL_FKExpectedSQL = q{

      MERGE INTO MART_TABLE_JOIN A USING
      (
      WITH C AS
        ( SELECT COUNT (*) AS rec_count
          FROM MART_TABLE_JOIN A
          WHERE
            A.TYPE = 'SAMPLE' AND
            NVL(A.CORE_FLG,'NULL') = 'Y' AND
            A.FROM_SCHEMA = 'JJJ' AND
            A.TO_SCHEMA = 'JJJ' AND
            A.FROM_TABLE = 'STORES' AND
            A.TO_TABLE = 'STORE_LOCATIONS'
        )
      SELECT mtj.*
      FROM  (
        SELECT
          'JJJ' as FROM_SCHEMA,
          'STORES' as FROM_TABLE,
          'LOCATION_ID' as FROM_FIELD,
          'JJJ' as TO_SCHEMA,
          'STORE_LOCATIONS' as TO_TABLE,
          'LOCATION_ID' as TO_FIELD,
          1 as FIELD_JOIN_ORD,
          'SAMPLE' as TYPE,
          'AUTO-GENERATED BY Unregistered JoinHero 2 using S_SL_FK' as NOTES,
          'Y' as CORE_FLG
        FROM DUAL
      ) mtj
        CROSS JOIN c
        WHERE c.rec_count = 0
      ) B
      ON (  A.TYPE = B.TYPE
        and NVL(A.CORE_FLG,'NULL1') = NVL(B.CORE_FLG,'NULL2')
        and A.FROM_SCHEMA = B.FROM_SCHEMA
        and A.TO_SCHEMA = B.TO_SCHEMA
        and A.FROM_TABLE = B.FROM_TABLE
        and A.TO_TABLE = B.TO_TABLE
        and A.FIELD_JOIN_ORD = B.FIELD_JOIN_ORD)
      WHEN NOT MATCHED THEN
      INSERT (
        FROM_SCHEMA, FROM_TABLE, FROM_FIELD, TO_SCHEMA, TO_TABLE,
        TO_FIELD, FIELD_JOIN_ORD, TYPE, NOTES, CORE_FLG)
      VALUES (
        B.FROM_SCHEMA, B.FROM_TABLE, B.FROM_FIELD, B.TO_SCHEMA, B.TO_TABLE,
        B.TO_FIELD, B.FIELD_JOIN_ORD, B.TYPE, B.NOTES, B.CORE_FLG);


      MERGE INTO MART_TABLE_JOIN_CARDINALITY A USING
      (
        SELECT
          'JJJ' as FROM_SCHEMA,
          'STORES' as FROM_TABLE,
          'JJJ' as TO_SCHEMA,
          'STORE_LOCATIONS' as TO_TABLE,
          'ONE' as CARDINALITY,
          'SAMPLE' as TYPE,
          'AUTO-GENERATED BY Unregistered JoinHero 2 using S_SL_FK' as NOTES,
          'Y' as CORE_FLG
        FROM DUAL
      ) B
      ON (
        A.TYPE = B.TYPE
        and NVL(A.CORE_FLG,'NULL1') = NVL(B.CORE_FLG,'NULL2')
        and A.FROM_SCHEMA = B.FROM_SCHEMA
        and A.TO_SCHEMA = B.TO_SCHEMA
        and A.FROM_TABLE = B.FROM_TABLE
        and A.TO_TABLE = B.TO_TABLE)
      WHEN NOT MATCHED THEN
      INSERT (
        FROM_SCHEMA, FROM_TABLE, TO_SCHEMA, TO_TABLE, CARDINALITY,
        TYPE, NOTES, CORE_FLG)
      VALUES (
        B.FROM_SCHEMA, B.FROM_TABLE, B.TO_SCHEMA, B.TO_TABLE, B.CARDINALITY,
        B.TYPE, B.NOTES, B.CORE_FLG);
  };

  # Let's test getJoinSQL. This guy uses component hash refs to generate merge SQL for a particular join
  my $fkKey              = 'S_SL_FK-_-JJJ.STORES-_-JJJ.STORE_LOCATIONS';
  my $getJoinSQLParams   = {pkComponents => $pk_01, fkComponents => $fk_01};
  my $getJoinSQL_S_SL_FK = JoinHero::getJoinSQL($fkKey, $getJoinSQLParams);
  ok(whitespaceInsensitiveCompare($getJoinSQL_S_SL_FKExpectedSQL, $getJoinSQL_S_SL_FK));

  # Should get a very similar answer for the outer getJoinSQL sub since this fk is the only fk we loaded up
  my $getOutputSQLParms    = {pkComponents => $pk_01, fkComponents => $fk_01};
  my $getOutputSQL_S_SL_FK = JoinHero::getOutputSQL($getJoinSQLParams);
  ok(whitespaceInsensitiveCompare("$getJoinSQL_S_SL_FKExpectedSQL\ncommit;\n", $getOutputSQL_S_SL_FK));

  return;
} ## end sub sqlGenerationS_SL_FKBasic

sub whitespaceInsensitiveCompare {
  my ($string1, $string2) = @_;
  my $stripWhitespaceRegEx = 'tr/ \n//dr';
  my $cleaned1             = $string1 =~ $stripWhitespaceRegEx;
  my $cleaned2             = $string2 =~ $stripWhitespaceRegEx;
  my $compare              = $cleaned1 eq $cleaned2 ? 1 : 0;

  return $compare;
} ## end sub whitespaceInsensitiveCompare
