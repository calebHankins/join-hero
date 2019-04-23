############################################################################################
#                       (C) Copyright 2018 Acxiom LLC
#                               All Rights Reserved.
############################################################################################
#
# Script: JoinHero.pm
# Author: Caleb Hankins - chanki
# Date:   2018-12-10
#
# Purpose: Oracle DDL parser for scraping PK/FK/Unique Key metadata describing table joins
#
############################################################################################
# MODIFICATION HISTORY
##----------------------------------------------------------------------------------------
# DATE        PROGRAMMER                   DESCRIPTION
##----------------------------------------------------------------------------------------
# 2018-12-10  Caleb Hankins - chanki       Initial Copy
############################################################################################

package JoinHero;

use warnings;
use strict;
use File::Glob ':glob';    # Perl extension for BSD glob routine
use Data::Dumper;
use File::Path qw(make_path);
use Graph;
no if $] >= 5.017011, warnings => 'experimental::smartmatch';    # Suppress smartmatch warnings

##--------------------------------------------------------------------------
# Version info
our $VERSION = '0.2.1';
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Create logger object
use JoinHero::Logger;
our $logger = JoinHero::Logger->new() or die "Cannot retrieve Logger object\n";
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Verbosity
our $verbose = 0;    # Default to not verbose
##--------------------------------------------------------------------------

##---------------------------------------------------------------------------
# Capture and save valuable components in the supplied DDL file
sub getKeyComponents {
  my ($rawDDL, $supportedMartsRef) = @_;
  $supportedMartsRef //= [];    # If we didn't get this argument, default to an empty array ref
  my @supportedMarts = @$supportedMartsRef;
  my $subName        = (caller(0))[3];

  # Describe what things look like
  my $validOracleObjectCharacterClasses = q{".a-zA-Z0-9_\$\@};                             # Oracle object
  my $validOracleFieldListClasses       = $validOracleObjectCharacterClasses . q{\,\s};    # Field list
  my $captureOracleObject               = qq{([$validOracleObjectCharacterClasses]+)};     # Capture Oracle objects
  my $captureFieldList                  = qq{([$validOracleFieldListClasses]+)};           # Capture Oracle field lists
  my $keyDDLRegEx                       = q{ALTER TABLE.*?;};                              # Key DDL

  # Capture FK components
  my $fkComponentsRegEx = q{};
  $fkComponentsRegEx .= q{ALTER TABLE[\s]*};
  $fkComponentsRegEx .= $captureOracleObject;                                              # Cap 1, to table name
  $fkComponentsRegEx .= q{[\s]*ADD CONSTRAINT[\s]*};
  $fkComponentsRegEx .= $captureOracleObject;                                              # Cap 2, fk name
  $fkComponentsRegEx .= q{[\s]*FOREIGN KEY[\s]*\([\s]*};
  $fkComponentsRegEx .= $captureFieldList;                                                 # Cap 3, to field list
  $fkComponentsRegEx .= q{[\s]*\)[\s]*REFERENCES[\s]+};
  $fkComponentsRegEx .= $captureOracleObject;                                              # Cap 4, from table name
  $fkComponentsRegEx .= q{[\s]*.*?[\s]*\([\s]*};
  $fkComponentsRegEx .= $captureFieldList;                                                 # Cap 5, from field list
  $fkComponentsRegEx .= q{[\s]*\)[\s]*;};
  if ($verbose) { $logger->info("$subName Parsing foreign keys using RegEx: $fkComponentsRegEx\n"); }

  # Capture PK / Unique components
  my $pkComponentsRegEx = q{};
  $pkComponentsRegEx .= q{ALTER TABLE[\s]*};
  $pkComponentsRegEx .= $captureOracleObject;                                              # Cap 1, table name
  $pkComponentsRegEx .= q{[\s]*ADD CONSTRAINT[\s]*};
  $pkComponentsRegEx .= $captureOracleObject;                                              # Cap 2, constraint name
  $pkComponentsRegEx .= q{[\s]*};
  $pkComponentsRegEx .= q{(PRIMARY KEY|UNIQUE)};                                           # Cap 3, constraint type
  $pkComponentsRegEx .= q{[\s]*\([\s]*};
  $pkComponentsRegEx .= $captureFieldList;                                                 # Cap 4, field list
  $pkComponentsRegEx .= q{[\s]*\)[\s]*;};
  if ($verbose) { $logger->info("$subName Parsing primary and unique keys using RegEx: $pkComponentsRegEx\n"); }

  my @keyDDL = $rawDDL =~ /$keyDDLRegEx/gms;    # Save off each Key DDL statement into its own array element

  # Munge our primary and unique keys into components that we can use
  my $pkComponents = {};                        # Hash ref to hold our broken out component parts
  for my $pk (@keyDDL) {
    if ($pk =~ /$pkComponentsRegEx/gms) {
      my $table     = $1;
      my $pkName    = $2;
      my $pkType    = $3;
      my $fieldList = $4;

      # Store components if we have all the information that we will need
      if ($table and $pkName and $pkType and $fieldList) {

        # Remove any whitespace characters from the field lists
        $fieldList =~ s/\s+//g;

        # Uppercase field list
        $fieldList = uc($fieldList);

        # Clean up names
        $table = getCleanedObjectName($table);

        # Save components
        $pkComponents->{$pkName}->{'pkName'}    = $pkName;
        $pkComponents->{$pkName}->{'table'}     = getTableName($table);
        $pkComponents->{$pkName}->{'schema'}    = getSchemaName($table, \@supportedMarts);
        $pkComponents->{$pkName}->{'pkType'}    = $pkType;
        $pkComponents->{$pkName}->{'fieldList'} = $fieldList;

        # Split and store field names as array elements
        @{$pkComponents->{$pkName}->{'fields'}} = split(',', $fieldList);
      } ## end if ($table and $pkName...)
    } ## end if ($pk =~ /$pkComponentsRegEx/gms)
  } ## end for my $pk (@keyDDL)

  # if ($verbose) { $logger->info("$subName Primary and unique key components:\n" . Dumper($pkComponents)); }

  # Munge our FKs into components that we can use
  my $fkComponents = {};    # Hash ref to hold our broken out component parts
  for my $fk (@keyDDL) {
    if ($fk =~ /$fkComponentsRegEx/gms) {
      my $fromTable     = $1;
      my $fromFieldList = $3;
      my $fkName        = $2;
      my $toTable       = $4;
      my $toFieldList   = $5;

      # Store components if we have all the information that we will need
      if ($toTable and $toFieldList and $fkName and $fromTable and $fromFieldList) {

        # Generating a key name that includes the from and to table name. Modeler tool allows for non-unique FK names...
        my $fkKey = "$fkName-_-$fromTable-_-$toTable";

        # Remove any whitespace characters from the field lists
        $fromFieldList =~ s/\s+//g;
        $toFieldList   =~ s/\s+//g;

        # Uppercase field lists
        $fromFieldList = uc($fromFieldList);
        $toFieldList   = uc($toFieldList);

        # Clean up names
        $fromTable = getCleanedObjectName($fromTable);
        $toTable   = getCleanedObjectName($toTable);

        # Save components
        $fkComponents->{$fkKey}->{'fkKey'}         = $fkKey;
        $fkComponents->{$fkKey}->{'fkName'}        = $fkName;
        $fkComponents->{$fkKey}->{'fromTable'}     = getTableName($fromTable);
        $fkComponents->{$fkKey}->{'fromFieldList'} = $fromFieldList;
        $fkComponents->{$fkKey}->{'toTable'}       = getTableName($toTable);
        $fkComponents->{$fkKey}->{'toFieldList'}   = $toFieldList;

        # Split and store field names as array elements
        @{$fkComponents->{$fkKey}->{'fromFields'}} = split(',', $fromFieldList);
        @{$fkComponents->{$fkKey}->{'toFields'}}   = split(',', $toFieldList);

        # Munge and set schema names using the table prefix
        my $fromSchema = getSchemaName($fromTable, \@supportedMarts);
        my $toSchema   = getSchemaName($toTable,   \@supportedMarts);

        # If we got one schema but not the other, set the empty one using the populated one
        if ($fromSchema && !$toSchema)   { $toSchema   = $fromSchema; }
        if ($toSchema   && !$fromSchema) { $fromSchema = $toSchema; }

        # Save munged schema components
        $fkComponents->{$fkKey}->{'fromSchema'} = $fromSchema;
        $fkComponents->{$fkKey}->{'toSchema'}   = $toSchema;

        # Calculate and store cardinality
        $fkComponents->{$fkKey}->{'cardinalityNORMAL'} = getJoinCardinality($pkComponents, $fkComponents->{$fkKey});
        $fkComponents->{$fkKey}->{'cardinalityREVERSED'}
          = getJoinCardinality($pkComponents, $fkComponents->{$fkKey}, 'REVERSED');
      } ## end if ($toTable and $toFieldList...)
    } ## end if ($fk =~ /$fkComponentsRegEx/gms)
  } ## end for my $fk (@keyDDL)

  # if ($verbose) { $logger->info("$subName Foreign keys components:\n" . Dumper($fkComponents)); }

  return ($pkComponents, $fkComponents);
} ## end sub getKeyComponents
##---------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Use component hash refs to generate SQL to update the MTJ(C) tables
sub getOutputSQL {
  my ($getOutputSQLParams) = @_;
  my $subName = (caller(0))[3];
  my $outputSQL;

  # Generate CREATE TABLE statements
  if ($getOutputSQLParams->{createTables}) {
    $outputSQL .= getJoinTableSQL($getOutputSQLParams);
    $outputSQL .= getCardinalityTableSQL($getOutputSQLParams);
  }

  # Check list of transforms, branch depending on transform needs
  my @supportedTypes;
  my @simpleTypes;
  $getOutputSQLParams->{simpleTypes} = \@simpleTypes;
  my @graphTypes;
  $getOutputSQLParams->{graphTypes} = \@graphTypes;
  if (defined $getOutputSQLParams->{supportedTypes}) { @supportedTypes = @{$getOutputSQLParams->{supportedTypes}}; }
  if (!@supportedTypes) { @supportedTypes = ('SAMPLE'); }

  # A type is optionally a colon delimited string in the format app[:transform]
  for my $typeString (@supportedTypes) {
    my ($type, $transform) = split(':', $typeString);
    $transform //= 'NORMAL';    # Default transform to NORMAL if not specified
    $transform = uc($transform);

    # For SNOWFLAKE and STAR transforms, switch to graph based generation. Else run through simple path
    my $graphRegEx = q{star|snowflake};
    if   ($transform =~ /$graphRegEx/gmi) { push(@graphTypes,  $typeString); }
    else                                  { push(@simpleTypes, $typeString); }
  } ## end for my $typeString (@supportedTypes)

  # Print type breakdown for debugging
  if ($verbose) {
    $logger->info(
            "$subName \@supportedTypes: [@supportedTypes], \@simpleTypes: [@simpleTypes], \@graphTypes: [@graphTypes]");
  }

  # Generate SQL for graph based types
  $outputSQL .= getGraphJoinSQL($getOutputSQLParams);    # Append simple join SQL

  # Generate SQL for simple types
  $outputSQL .= getSimpleJoinSQL($getOutputSQLParams);    # Append simple join SQL

  $outputSQL .= "\ncommit;\n";

  return $outputSQL;
} ## end sub getOutputSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Generate and return join SQL for all relevant graph based types
sub getGraphJoinSQL {
  my ($getGraphJoinSQLParams) = @_;
  my $subName                 = (caller(0))[3];
  my $outputSQL               = '';

  my $commitThreshold = $getGraphJoinSQLParams->{commitThreshold};
  $commitThreshold //= 1000;    # Default if not supplied
  my $uncommittedTransactions = $getGraphJoinSQLParams->{uncommittedTransactions};
  $uncommittedTransactions //= 0;    # Default if not supplied

  # Alias parms for ease of use
  my @types = @{$getGraphJoinSQLParams->{graphTypes}};

  if (@types) {

    # Generate a graph
    my $g = getGraph($getGraphJoinSQLParams);

    # Loop over graph types and collect join SQL
    for my $typeString (@types) {
      if ($verbose) { $logger->info("$subName processing: $typeString"); }

      my $transform = {};
      $transform->{typeString} = $typeString;

      # A graph typeString is a colon delimited string in the format app[:transform]
      my ($app, $transformString) = split(':', $typeString);
      $transform->{app} = $app;
      $transformString = uc($transformString);

      # Split apart our transform into its components
      my $transformTypeRegEx         = '(STAR|SNOWFLAKE)';
      my $transformToTableDelimRegEx = '(->)';
      my $tableNameRegEx             = '([".a-zA-Z0-9_\$\@]+)';
      my $tableToMaxDepthDelimRegEx  = '(->)?';
      my $maxDepthRegEx              = '([0-9]*)';
      my $graphGrokVoltronRegEx      = q{};
      $graphGrokVoltronRegEx .= $transformTypeRegEx;
      $graphGrokVoltronRegEx .= $transformToTableDelimRegEx;
      $graphGrokVoltronRegEx .= $tableNameRegEx;
      $graphGrokVoltronRegEx .= $tableToMaxDepthDelimRegEx;
      $graphGrokVoltronRegEx .= $maxDepthRegEx;

      # Set default maxDepth
      $transform->{maxDepth} = 5;

      # Use regex to save off relevant transform components
      if ($transformString =~ /$graphGrokVoltronRegEx/gms) {
        $transform->{transformType} = $1;
        $transform->{tableName}     = $3;
        $transform->{maxDepth}      = $5;
      }

      # Stars are a special case and have a cap of 1
      if ($transform->{transformType} eq 'STAR') { $transform->{maxDepth} = 1; }

      # Print debug info
      if ($verbose) { $logger->info("$subName \$transform: " . Dumper($transform)); }

      # Fetch the join list
      my @joinList = recursiveGetSuccessors(
                               {fullGraph => $g, v => $transform->{tableName}, iterationCap => $transform->{maxDepth}});

      # Dedupe join list
      my @joinListUniq = getUniqArray(@joinList);

      # Get SQL for the joins
      # Shallow copy to avoid mutation
      my %getSQLForJoinPathsParms = %{$getGraphJoinSQLParams};
      $getSQLForJoinPathsParms{transform} = $transform;
      $getSQLForJoinPathsParms{paths}     = \@joinListUniq;

      # if ($verbose) { $logger->info("$subName \%getSQLForJoinPathsParms" . Dumper(%getSQLForJoinPathsParms)) }

      $outputSQL .= getSQLForJoinPaths(\%getSQLForJoinPathsParms);
    } ## end for my $typeString (@types)

  } ## end if (@types)
  else {
    if ($verbose) {
      $logger->info("$subName No graphTypes detected, skipping graph based join SQL generation.");
    }
  }

  $getGraphJoinSQLParams->{uncommittedTransactions} = $uncommittedTransactions;

  return $outputSQL;
} ## end sub getGraphJoinSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
#  Recursively traverse a given graph building pairs of joins from visited edges
sub recursiveGetSuccessors {
  my ($recursiveGetSuccessorsParms) = @_;
  my $subName = (caller(0))[3];

  # Alias parms for ease of use
  my $fullGraph        = $recursiveGetSuccessorsParms->{fullGraph};
  my $v                = $recursiveGetSuccessorsParms->{v};
  my $parents          = $recursiveGetSuccessorsParms->{parents};
  my $currentIteration = $recursiveGetSuccessorsParms->{currentIteration};
  my $iterationCap     = $recursiveGetSuccessorsParms->{iterationCap};

  $parents //= [$v];    # The whole chain that brung us
  my @parents = @$parents;

  @parents = getUniqArray(@parents);
  $currentIteration //= 1;    # Assume we're the first unless told otherwise
  $iterationCap     //= 5;    # Don't recurse further than this by default

  if ($verbose) {
    $logger->info(
          "$subName for:$v. \$currentIteration: $currentIteration out of a capped $iterationCap and a parent list of:\n"
            . Dumper(@parents));
  }
  my @cleansedSuccessors = ();
  my @joinList           = ();

  if ($currentIteration > $iterationCap) {

    if ($verbose) {
      $logger->info(
                  "$subName exceeded max iteration cap, bailing out $currentIteration out of a capped $iterationCap\n");
    }
    return @joinList;
  } ## end if ($currentIteration ...)

  my @successors = $fullGraph->successors($v);

  # Add the valid successors for $v
  for my $successor (@successors) {
    if ($successor ne $v && !($successor ~~ @parents)) {    # Don't loop back
      push(@joinList, [$v, $successor]);
      push(@cleansedSuccessors, $successor);
    }

    else {
      if ($verbose) {
        $logger->info("$subName $v successor $successor skipped due to not meeting cleansedSuccessor status.\n");
      }
    }

  } ## end for my $successor (@successors)

  # Now for each of the successors, need to calculate their successors
  @cleansedSuccessors = sort @cleansedSuccessors;    # Sort these so it is deterministic

  if ($verbose) {
    $logger->info("$subName for:$v \@cleansedSuccessors:\n" . Dumper(@cleansedSuccessors));
    $logger->info("$subName for:$v \@joinList before recursion:\n" . Dumper(@joinList));
  }

  # Then go down the rabbit hole for $v's kids if we haven't hit the cap yet
  if ($currentIteration + 1 <= $iterationCap) {
    for my $cleansedSuccessor (@cleansedSuccessors) {
      if ($verbose) {
        $logger->info("$subName down the rabbit hole for $v \$cleansedSuccessor:$cleansedSuccessor\n");
      }
      my @kidsParents = @parents;
      push(@kidsParents, $v);
      push(
           @joinList,
           recursiveGetSuccessors(
                                  {
                                   fullGraph        => $fullGraph,
                                   v                => $cleansedSuccessor,
                                   parents          => \@kidsParents,
                                   currentIteration => $currentIteration + 1,
                                   iterationCap     => $iterationCap
                                  }
           )
      );
    } ## end for my $cleansedSuccessor...
    if ($verbose) { $logger->info("$subName for:$v \@joinList after recursion:\n" . Dumper(@joinList)) }
  } ## end if ($currentIteration ...)

  return @joinList;

} ## end sub recursiveGetSuccessors
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Given a 2d array of join paths, return a SQL string of join metadata
sub getSQLForJoinPaths {
  my ($getSQLForJoinPathsParms) = @_;
  my $subName                   = (caller(0))[3];
  my $outputSQL                 = '';

  # Alias our params for easier use
  my $paths = $getSQLForJoinPathsParms->{paths};

  for my $path (@{${paths}}) {
    my @joinPair = @{$path};

    my $join = getJoinFromComponents(
                                     {
                                      fromTable    => $joinPair[0],
                                      toTable      => $joinPair[1],
                                      pkComponents => $getSQLForJoinPathsParms->{pkComponents},
                                      fkComponents => $getSQLForJoinPathsParms->{fkComponents}
                                     }
    );

    my %getJoinSQLParms = %{$getSQLForJoinPathsParms};

    # set overrideTypes for this join
    my $typeString = "$getJoinSQLParms{transform}->{app}:$join->{direction}";
    $getJoinSQLParms{overrideTypes} = [$typeString];

    # if ($verbose) {
    #   $logger->info("$subName \$getJoinSQLParms{overrideTypes}" . Dumper($getJoinSQLParms{overrideTypes}));
    # }

    my $joinSQL = getJoinSQL($join->{join}->{fkKey}, \%getJoinSQLParms);

    $outputSQL .= $joinSQL;

  } ## end for my $path (@{${paths...}})

  return $outputSQL;
} ## end sub getSQLForJoinPaths
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Use fkComponents to produce a weighted and direct graph
sub getGraph {
  my ($getGraphParams) = @_;
  my $subName = (caller(0))[3];

  my $g  = Graph->new(directed => 1);          # A directed graph.
  my $fk = $getGraphParams->{fkComponents};    # Alias fkComponents for ease of use

  if (!defined $fk) {
    $logger->error("$subName was asked to generate a graph but wasn't given a hashref containing valid fkComponents!");
    return $g;
  }

  # Loop over every fk, add vertexes and edges to our graph
  while (my ($key, $value) = each %{$fk}) {
    if (!defined($value->{fromSchema})) { next; }

    # Set edge info and weight based on NORMAL direction
    my $edgeWeightNormal = $value->{cardinalityNORMAL} eq 'ONE' ? 1 : 10;    # Weight paths to prefer 1-1 joins
    my %tempAttr = %{$value};    # make a shallow copy of this hash ref so we don't mutate it
    $tempAttr{weight}    = $edgeWeightNormal;    # Assign a weight
    $tempAttr{direction} = 'NORMAL';             # Assign a 'direction'
    $g->set_edge_attributes($value->{toTable}, $value->{fromTable}, \%tempAttr);

    # Set edge info and weight based on REVERSED direction
    my $edgeWeightReversed = $value->{cardinalityREVERSED} eq 'ONE' ? 1 : 10;    # Weight paths to prefer 1-1 joins
    my %tempAttrReversed = %{$value};    # make a shallow copy of this hash ref so we don't mutate it
    $tempAttrReversed{weight}    = $edgeWeightReversed;    # Assign a weight
    $tempAttrReversed{direction} = 'REVERSED';             # Assign a 'direction'
    $g->set_edge_attributes($value->{fromTable}, $value->{toTable}, \%tempAttrReversed);
  } ## end while (my ($key, $value) ...)

  return $g;
} ## end sub getGraph
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Generate and return join SQL for all relevant simple types
sub getSimpleJoinSQL {
  my ($getSimpleJoinSQLParams) = @_;
  my $subName                  = (caller(0))[3];
  my $outputSQL                = '';

  my $commitThreshold = $getSimpleJoinSQLParams->{commitThreshold};
  $commitThreshold //= 1000;    # Default if not supplied
  my $uncommittedTransactions = $getSimpleJoinSQLParams->{uncommittedTransactions};
  $uncommittedTransactions //= 0;    # Default if not supplied

  if (@{$getSimpleJoinSQLParams->{simpleTypes}}) {

    # Step through each FK and generate SQL for each. Append to working SQL variable each iteration
    for my $key (sort keys %{$getSimpleJoinSQLParams->{fkComponents}}) {
      my $joinSQL = getJoinSQL($key, $getSimpleJoinSQLParams);
      if ($joinSQL) {
        $outputSQL .= $joinSQL;
        $uncommittedTransactions += () = $joinSQL =~ /;/g;    # Count semicolons to determine transactions added
        if ($verbose) { $logger->info("$subName uncommittedTransactions: $uncommittedTransactions\n"); }
        if ($uncommittedTransactions > $commitThreshold) {    # If we've reached the threshold, commit and reset count
          $logger->info("$subName Reached transaction threshold, inserting commit\n");
          $outputSQL .= "\ncommit;\n";
          $uncommittedTransactions = 0;
        }
      } ## end if ($joinSQL)
    } ## end for my $key (sort keys ...)
  } ## end if (@{$getSimpleJoinSQLParams...})
  else {
    if ($verbose) {
      $logger->info("$subName No simpleTypes detected, skipping simple join SQL generation.");
    }
  }

  $getSimpleJoinSQLParams->{uncommittedTransactions} = $uncommittedTransactions;

  return $outputSQL;
} ## end sub getSimpleJoinSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Generate and return SQl for the table that will contain the joins and join fields
sub getJoinTableSQL {
  my ($getJoinTableSQLParams) = @_;
  my $subName                 = (caller(0))[3];
  my $outputSQL               = '';

  my $martTableJoinTableName = $getJoinTableSQLParams->{martTableJoinTableName};

  $outputSQL .= qq{
  CREATE TABLE $martTableJoinTableName
  (
      FROM_SCHEMA       VARCHAR2 (100),
      FROM_TABLE        VARCHAR2 (100),
      FROM_FIELD        VARCHAR2 (100),
      TO_SCHEMA         VARCHAR2 (100),
      TO_TABLE          VARCHAR2 (100),
      TO_FIELD          VARCHAR2 (100),
      FIELD_JOIN_ORD    NUMBER,
      TYPE              VARCHAR2 (100),
      NOTES             VARCHAR2 (4000),
      CORE_FLG          VARCHAR2 (4000),
      PRIMARY KEY
          (TYPE,
          FROM_SCHEMA,
          TO_SCHEMA,
          FROM_TABLE,
          TO_TABLE,
          FIELD_JOIN_ORD)
  )};
  $outputSQL .= ";\n";

  return $outputSQL;
} ## end sub getJoinTableSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Generate and return SQl for the table that will contain the join cardinality
sub getCardinalityTableSQL {
  my ($getCardinalityTableSQLParams) = @_;
  my $subName                        = (caller(0))[3];
  my $outputSQL                      = '';

  my $martCardinalityTableName = $getCardinalityTableSQLParams->{martCardinalityTableName};

  $outputSQL .= qq{
  CREATE TABLE $martCardinalityTableName
  (
    FROM_SCHEMA    VARCHAR2 (100),
    FROM_TABLE     VARCHAR2 (100),
    TO_SCHEMA      VARCHAR2 (100),
    TO_TABLE       VARCHAR2 (100),
    CARDINALITY    VARCHAR2 (4000),
    TYPE           VARCHAR2 (100),
    NOTES          VARCHAR2 (4000),
    CORE_FLG       VARCHAR2 (4000),
    PRIMARY KEY
        (TYPE,
          FROM_SCHEMA,
          TO_SCHEMA,
          FROM_TABLE,
          TO_TABLE)
  )};
  $outputSQL .= ";\n";

  return $outputSQL;
} ## end sub getCardinalityTableSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Use component hash refs to generate merge SQL for a particular join
sub getJoinSQL {
  my ($fkKey, $getJoinSQLParams) = @_;
  my $subName   = (caller(0))[3];
  my $outputSQL = '';

  # Alias our params for easier use
  my $pkComponents             = $getJoinSQLParams->{pkComponents};
  my $fkComponents             = $getJoinSQLParams->{fkComponents};
  my $deleteExisting           = $getJoinSQLParams->{deleteExisting};
  my $updateExisting           = $getJoinSQLParams->{updateExisting};
  my $martTableJoinTableName   = $getJoinSQLParams->{martTableJoinTableName};
  my $martCardinalityTableName = $getJoinSQLParams->{martCardinalityTableName};
  my $coreFlg                  = $getJoinSQLParams->{coreFlg};
  my $allowUnknownSchema       = $getJoinSQLParams->{allowUnknownSchema};
  my @types;
  my @supportedMarts;

  # Set defaults for things we didn't get but need
  if (defined $getJoinSQLParams->{overrideTypes}) {
    if (@{$getJoinSQLParams->{overrideTypes}} > 0) { @types = @{$getJoinSQLParams->{overrideTypes}}; }
  }
  elsif (defined $getJoinSQLParams->{simpleTypes}) {
    if (@{$getJoinSQLParams->{simpleTypes}} > 0) { @types = @{$getJoinSQLParams->{simpleTypes}}; }
  }
  elsif (defined $getJoinSQLParams->{supportedTypes}) {
    if (@{$getJoinSQLParams->{supportedTypes}} > 0) { @types = @{$getJoinSQLParams->{supportedTypes}}; }
  }
  if (!@types)                                     { @types          = ('SAMPLE'); }
  if (defined $getJoinSQLParams->{supportedMarts}) { @supportedMarts = @{$getJoinSQLParams->{supportedMarts}}; }
  if (!@supportedMarts)                            { @supportedMarts = ('SAMPLE'); }
  $deleteExisting           //= 0;
  $updateExisting           //= 0;
  $martTableJoinTableName   //= 'MART_TABLE_JOIN';
  $martCardinalityTableName //= 'MART_TABLE_JOIN_CARDINALITY';
  $coreFlg                  //= 'Y';

  if ($verbose) { $logger->info("$subName Processing:$fkComponents->{$fkKey}->{fkKey} for types @types...\n"); }
  for my $typeString (@types) {

    # A typeString is optionally a colon delimited string in the format app[:direction]
    my ($type, $typeDirection) = split(':', $typeString);
    $typeDirection //= 'NORMAL';    # Default typeDirection to normal if not specified
    $typeDirection = uc($typeDirection);

    # Leave early if we have an unsupported typeDirection
    if ($typeDirection ne 'NORMAL' and $typeDirection ne 'REVERSED') { next; }

    if ($verbose) {
      $logger->info(
                  "$subName Processing:$fkComponents->{$fkKey}->{fkKey} for type $type, direction $typeDirection...\n");
    }

    # App specific init
    my $fromSchema;
    my $fromTable;
    my $toSchema;
    my $toTable;
    my $directionNote = '';
    if ($typeDirection eq 'REVERSED') {
      $fromSchema    = $fkComponents->{$fkKey}->{'toSchema'};
      $fromTable     = $fkComponents->{$fkKey}->{'toTable'};
      $toSchema      = $fkComponents->{$fkKey}->{'fromSchema'};
      $toTable       = $fkComponents->{$fkKey}->{'fromTable'};
      $directionNote = " in $typeDirection mode";
    } ## end if ($typeDirection eq ...)
    else {
      $fromSchema = $fkComponents->{$fkKey}->{'fromSchema'};
      $fromTable  = $fkComponents->{$fkKey}->{'fromTable'};
      $toSchema   = $fkComponents->{$fkKey}->{'toSchema'};
      $toTable    = $fkComponents->{$fkKey}->{'toTable'};
    } ## end else [ if ($typeDirection eq ...)]

    # Validate schema, set default if empty
    if (!defined($toSchema) || !defined($fromSchema)) {
      $toSchema   = 'UNKNOWN';
      $fromSchema = 'UNKNOWN';
      if (!$allowUnknownSchema) { return; }    # Leave early unless we allow UNKNOWN schema
    }

    # Exit early if the schema don't match, no cross mart joins
    if ($toSchema ne $fromSchema) {
      if ($verbose) {
        $logger->info("$subName Cross schema ($toSchema to $fromSchema), skipping\n");
      }
      next;
    } ## end if ($toSchema ne $fromSchema)

    # Generate MTJ record(s)
    if ($deleteExisting) {
      my $deleteSQLMartTableJoin = qq{
      DELETE FROM $martTableJoinTableName A
      WHERE
        A.TYPE = '$type' AND
        NVL(A.CORE_FLG,'NULL') = '$coreFlg' AND
        A.FROM_SCHEMA = '$fromSchema' AND
        A.TO_SCHEMA = '$toSchema' AND
        A.FROM_TABLE = '$fromTable' AND
        A.TO_TABLE = '$toTable';\n};

      if ($verbose) { $logger->info("$subName Adding deleteSQLMartTableJoin:\n$deleteSQLMartTableJoin\n"); }

      $outputSQL .= $deleteSQLMartTableJoin;
    } ## end if ($deleteExisting)

    my $mergeSQLData = '';    # Variable to hold the data set we'll merge into MTJ
    my $i            = 0;     # Setup a loop counter to use for field indexing and numbering
    for my $fkToField (@{$fkComponents->{$fkKey}->{'toFields'}}) {
      if ($verbose) {
        $logger->info(
                     "$subName Processing:$fkComponents->{$fkKey}->{fkKey} for type $type and toField $fkToField...\n");
      }
      my $fkFromField  = @{$fkComponents->{$fkKey}->{'fromFields'}}[$i];    # Grab the matching from field
      my $fieldJoinOrd = $i + 1;

      # Determine the to and from fields
      my $fromField;
      my $toField;
      if ($typeDirection eq 'REVERSED') {
        $fromField = $fkToField;
        $toField   = $fkFromField;
      }
      else {                                                                # Default to the supplied join order
        $fromField = $fkFromField;
        $toField   = $fkToField;
      }

      if ($i > 0) { $mergeSQLData .= qq{\n        UNION}; }
      $mergeSQLData .= qq{
        SELECT
          '$fromSchema' as FROM_SCHEMA,
          '$fromTable' as FROM_TABLE,
          '$fromField' as FROM_FIELD,
          '$toSchema' as TO_SCHEMA,
          '$toTable' as TO_TABLE,
          '$toField' as TO_FIELD,
          $fieldJoinOrd as FIELD_JOIN_ORD,
          '$type' as TYPE,
          'AUTO-GENERATED BY JoinHero Version: $VERSION using $fkComponents->{$fkKey}->{fkName}$directionNote' as NOTES,
          '$coreFlg' as CORE_FLG
        FROM DUAL};

      $i++;    # Record that we added a join record
    } ## end for my $fkToField (@{$fkComponents...})

    # Construct the MTJ merge statement
    my $mergeSQLMartTableJoinExistingCheckHeader = '';
    my $mergeSQLMartTableJoinExistingCheckFooter = '';
    if (!$deleteExisting) {

      # If we aren't clearing out the existing join metadata,
      # make sure it doesn't exist in any form before trying to insert
      $mergeSQLMartTableJoinExistingCheckHeader = qq{
      WITH C AS
        ( SELECT COUNT (*) AS rec_count
          FROM $martTableJoinTableName A
          WHERE
            A.TYPE = '$type' AND
            NVL(A.CORE_FLG,'NULL') = '$coreFlg' AND
            A.FROM_SCHEMA = '$fromSchema' AND
            A.TO_SCHEMA = '$toSchema' AND
            A.FROM_TABLE = '$fromTable' AND
            A.TO_TABLE = '$toTable'
        )
      SELECT mtj.*
      FROM  (};

      # Only attempt to update without a delete if our field count is less than or equal to the existing count
      my $mergeSQLMartTableJoinExistingCheckFooterCrossJoinClause = 'c.rec_count = 0';
      if ($updateExisting) { $mergeSQLMartTableJoinExistingCheckFooterCrossJoinClause = qq{c.rec_count <= $i}; }

      $mergeSQLMartTableJoinExistingCheckFooter = qq{
      ) mtj
        CROSS JOIN c
        WHERE $mergeSQLMartTableJoinExistingCheckFooterCrossJoinClause};
    } ## end if (!$deleteExisting)

    my $mergeSQLMartTableJoin = qq{
      MERGE INTO $martTableJoinTableName A USING
      ($mergeSQLMartTableJoinExistingCheckHeader $mergeSQLData $mergeSQLMartTableJoinExistingCheckFooter
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
        B.TO_FIELD, B.FIELD_JOIN_ORD, B.TYPE, B.NOTES, B.CORE_FLG)};

    if ($updateExisting) {
      $mergeSQLMartTableJoin .= qq{
        WHEN MATCHED THEN
        UPDATE SET
          A.FROM_FIELD = B.FROM_FIELD,
          A.TO_FIELD = B.TO_FIELD,
          A.NOTES = B.NOTES};
    } ## end if ($updateExisting)

    $mergeSQLMartTableJoin .= ";\n";

    if ($verbose) { $logger->info("$subName Adding mergeSQLMartTableJoin:\n$mergeSQLMartTableJoin\n"); }

    $outputSQL .= $mergeSQLMartTableJoin;

    # Add cardinality record
    my $cardinality = $fkComponents->{$fkKey}->{"cardinality$typeDirection"};

    if ($deleteExisting) {
      my $deleteSQLMartTableJoinCardinality = qq{
      DELETE FROM $martCardinalityTableName A
      WHERE
        A.TYPE = '$type' AND
        NVL(A.CORE_FLG,'NULL') = '$coreFlg' AND
        A.FROM_SCHEMA = '$fromSchema' AND
        A.TO_SCHEMA = '$toSchema' AND
        A.FROM_TABLE = '$fromTable' AND
        A.TO_TABLE = '$toTable';\n};

      if ($verbose) {
        $logger->info("$subName Adding deleteSQLMartTableJoinCardinality:\n$deleteSQLMartTableJoinCardinality\n");
      }

      $outputSQL .= $deleteSQLMartTableJoinCardinality;
    } ## end if ($deleteExisting)

    my $mergeSQLMartTableJoinCardinality = qq{
      MERGE INTO $martCardinalityTableName A USING
      (
        SELECT
          '$fromSchema' as FROM_SCHEMA,
          '$fromTable' as FROM_TABLE,
          '$toSchema' as TO_SCHEMA,
          '$toTable' as TO_TABLE,
          '$cardinality' as CARDINALITY,
          '$type' as TYPE,
          'AUTO-GENERATED BY JoinHero Version: $VERSION using $fkComponents->{$fkKey}->{fkName}$directionNote' as NOTES,
          '$coreFlg' as CORE_FLG
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
        B.TYPE, B.NOTES, B.CORE_FLG)};

    if ($updateExisting) {
      $mergeSQLMartTableJoinCardinality .= qq{
        WHEN MATCHED THEN
        UPDATE SET
          A.CARDINALITY = B.CARDINALITY,
          A.NOTES = B.NOTES};
    } ## end if ($updateExisting)

    $mergeSQLMartTableJoinCardinality .= ";\n";

    if ($verbose) {
      $logger->info("$subName Adding mergeSQLMartTableJoinCardinality:\n$mergeSQLMartTableJoinCardinality\n");
    }

    $outputSQL .= $mergeSQLMartTableJoinCardinality;

  } ## end for my $typeString (@types)

  return $outputSQL;
} ## end sub getJoinSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Calculate a given join's cardinality
sub getJoinCardinality {
  my ($pkComponents, $join, $direction) = @_;
  my $subName = (caller(0))[3];
  $direction //= 'to';    # Default direction if we didn't get one
  $direction = uc($direction) eq 'REVERSED' ? 'from' : $direction;    # Allow 'REVERSED' as well as a flip toggle
  if ($direction ne 'to' and $direction ne 'from') { return 'INVALID_DIRECTION'; }
  my @joinFields = sort($join->{"${direction}Fields"});
  my $cardinality = 'MANY';    # Default cardinality to MANY, we'll override later if we have a key match

  # If the join's toFields match a unique key for toTable, flag that join as 1-1
  for my $pkKey (sort keys %{$pkComponents}) {
    if ($join->{"${direction}Table"} eq $pkComponents->{$pkKey}->{'table'}) {
      my @pkFields = sort($pkComponents->{$pkKey}->{'fields'});
      if (@joinFields ~~ @pkFields) { $cardinality = 'ONE'; last; }    # If we found a key match, leave early
    }
  } ## end for my $pkKey (sort keys...)

  return $cardinality;
} ## end sub getJoinCardinality
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
#
sub getCleanedObjectName {
  my ($objectName) = @_;
  my $subName = (caller(0))[3];
  my $cleanedObjectName;

  # If we got a name wrapped in quotes, strip them off but preserve the casing
  if ($objectName =~ /"/gm) {
    $cleanedObjectName = stripQuotes($objectName);
  }
  else {
    # Else uppercase and move on
    $cleanedObjectName = uc($objectName);
  }

  return $cleanedObjectName;
} ## end sub getCleanedObjectName
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Derive a table name, apply any digital transformation services needed for translation
sub getTableName {
  my ($objectName) = @_;
  my $subName = (caller(0))[3];
  my $tableName;

  # If we got a dot in the object name, take the second half, else take the original
  if ($objectName =~ /([a-zA-Z0-9_\$\@]*)(\.?)([a-zA-Z0-9_\$\@]*)/) {
    if   ($2) { $tableName = $3; }
    else      { $tableName = $objectName; }
  }

  # If we got some variant of recipient, just return recipient
  $tableName =~ /(RECIPIENT)/gms;
  if ($1 and $1 eq 'RECIPIENT') { $tableName = 'RECIPIENT'; }

  return $tableName;
} ## end sub getTableName
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Derive the schema name using the table name
# Use the first part of the table name up to the first underscore for the schema name
sub getSchemaName {
  my ($objectName, $supportedMartsRef) = @_;
  $supportedMartsRef //= [];    # If we didn't get this argument, default to an empty array ref
  my @supportedMarts = @$supportedMartsRef;
  my $schemaName;

  # Try to use dot delimiter to carve off a schema name
  if ($objectName =~ /([a-zA-Z0-9_\$\@]*)(\.?)([a-zA-Z0-9_\$\@]*)/) {
    if ($2) { $schemaName = $1; }
    else {    # If we didn't have a dot delimiter, to the use the name up to the first underscore for the schema

      # Take the first token using underscore delimiter
      $schemaName = ($objectName =~ /([a-zA-Z0-9]+?)_.*/)[0];

      # CMP prefixes are special and actually mean CMP_DM
      if (defined $schemaName) {
        if ($schemaName eq 'CMP') { $schemaName = 'CMP_DM'; }
      }

      # Check schema is in our whitelist. If not, unset the value
      if (!($schemaName ~~ @supportedMarts)) { $schemaName = undef; }
    } ## end else [ if ($2) ]
  } ## end if ($objectName =~ /([a-zA-Z0-9_\$\@]*)(\.?)([a-zA-Z0-9_\$\@]*)/)

  return $schemaName;
} ## end sub getSchemaName
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Lookup and return a particular join based on a from and to table and supplied components
# Returns:
#   A hash ref with a key of 'join' with a value of the requested join
#   and a key of direction with a value of the join direction (based on the supplied from/to relative to the join)
sub getJoinFromComponents {
  my ($getJoinFromComponentsParams) = @_;
  my $subName                       = (caller(0))[3];
  my $join                          = {};
  my $direction;

  # Alias our params for easier use
  my $fromTable    = $getJoinFromComponentsParams->{fromTable};
  my $toTable      = $getJoinFromComponentsParams->{toTable};
  my $pkComponents = $getJoinFromComponentsParams->{pkComponents};
  my $fkComponents = $getJoinFromComponentsParams->{fkComponents};

  # Search for our requested join (NORMAL and REVERSE style)
  for my $key (sort keys %{$fkComponents}) {

    # Prefer 'NORMAL' style joins if we can find them
    if ($fkComponents->{$key}->{fromTable} eq $fromTable and $fkComponents->{$key}->{toTable} eq $toTable) {
      $join      = $fkComponents->{$key};
      $direction = 'NORMAL';
      last;
    }

    # Otherwise, we'll take a 'REVERSED' join if we have to
    elsif ($fkComponents->{$key}->{toTable} eq $fromTable and $fkComponents->{$key}->{fromTable} eq $toTable) {
      $join      = $fkComponents->{$key};
      $direction = 'REVERSED';
      last;
    }
  } ## end for my $key (sort keys ...)

  if (!defined($direction)) {
    $logger->warn("$subName could not find a join for fromTable:$fromTable toTable:$toTable");
  }

  return {join => $join, direction => $direction};
} ## end sub getJoinFromComponents
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Return a version of the supplied string, but without any double quote characters
sub stripQuotes {
  my ($string) = @_;
  my $stripped = $string;
  $stripped =~ s/"//gm;
  return $stripped;
} ## end sub stripQuotes
##-------------------------------------------------------------------------

##--------------------------------------------------------------------------
sub openAndLoadFile {
  my ($filename)   = @_;
  my $subName      = (caller(0))[3];
  my $fileContents = '';

  # Try to open our file
  open my $fileHandle, "<", $filename or $logger->confess("$subName Could not open file '$filename' $!");

  # Read file handle data stream into our file variable
  while (defined(my $line = <$fileHandle>)) {
    $fileContents .= $line;
  }

  close($fileHandle);

  if (length($fileContents) <= 0) {
    $logger->confess(
             "$subName It appears that nothing was in [$filename] Please check file and see if it meets expectations.");
  }

  return $fileContents;
} ## end sub openAndLoadFile
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Take our file data and use it to create a file on the filesystem
sub createExportFile {
  my (
      $fileData,          # File contents to write out
      $exportFilepath,    # Might be full file path or just dir
      $exportFilename,    # Might be undef or just the filename
      $utfDisabled        # Optional indicator to avoid utf-8 encoding
  ) = @_;
  my $subName            = (caller(0))[3];
  my $exportFileFullName = $exportFilename ? "${exportFilepath}/${exportFilename}" : $exportFilepath;
  my $filepathParts      = getFilepathParts($exportFileFullName);

  # Create the dir if it doesn't already exist
  eval { make_path($filepathParts->{'dirname'}) };
  $logger->confess("$subName Could not make directory: $filepathParts->{'dirname'}: $@") if $@;

  $logger->info("$subName Attempting creation of file [$exportFileFullName]\n");
  open my $exportFile, q{>}, $exportFileFullName
    or $logger->confess("$subName Could not open file: $exportFileFullName: $!\n")
    ;    # Open file, overwrite if exists, raise error if we run into trouble
  if (!$utfDisabled) { binmode($exportFile, ":encoding(UTF-8)") }
  print $exportFile $fileData;
  close($exportFile);
  $logger->info("$subName Success!\n");

  return;
} ## end sub createExportFile
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Get and return a hash ref of file name parts
sub getFilepathParts {
  my ($filename, @extList) = @_;
  my $subName = (caller(0))[3];

  my ($name, $dirname, $ext);
  eval { ($name, $dirname, $ext) = File::Basename::fileparse($filename, @extList); };
  $logger->confess("$subName Could not fileparse '$filename'. Error message from fileparse: '$@'") if $@;

  return {
          'dirname'  => $dirname,
          'name'     => $name,
          'ext'      => $ext,
          'basename' => $name . $ext,
          'fullpath' => $dirname . $name . $ext
  };
} ## end sub getFilepathParts
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Get unique array
# Serializing, adding serialized version to a hash and checking against the hash
# to see if we've seen the element before.  curtesy of perlfaq4 + Dumper for serialization
sub getUniqArray {
  my (@array) = @_;
  my %seen = ();
  my @unique = grep { !$seen{Dumper($_)}++ } @array;

  return @unique;
} ## end sub getUniqArray
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Make sure required parm is populated
sub checkRequiredParm {
  my ($requiredParmVal, $requiredParmValName, $errMsg) = @_;
  my $parentName = (caller(1))[3];
  $requiredParmValName //= "Missing a parameter that";
  $errMsg              //= "$requiredParmValName is required.";
  my $errorCnt = 0;

  # Make sure value is populated
  unless (defined($requiredParmVal) and length($requiredParmVal) > 0) {
    $logger->error("$parentName $errMsg\n");
    $errorCnt++;
  }

  return $errorCnt;
} ## end sub checkRequiredParm
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Print sign off message and return a status code
sub signOff {
  my (
      $statusCode,      # Optional $? (statusCode)
      $inErrorCount,    # Optional supplemental error count if not using logger
      $inWarnCount      # Optional supplemental warning count if not using logger
  ) = @_;
  $statusCode   //= 0;    # Default to 0 if not supplied
  $inErrorCount //= 0;    # Default to 0 if not supplied
  $inWarnCount  //= 0;    # Default to 0 if not supplied
  my $parentName = (caller(1))[3];    # Calling sub name
  $parentName //= 'main::main';       # Default parentName if we couldn't find one

  my $errorCount   = $logger->get_count("ERROR") + $inErrorCount;    # Combine logger's count and the supplied count
  my $warningCount = $logger->get_count("WARN") + $inWarnCount;      # Combine logger's count and the supplied count

  if (!$statusCode && $errorCount) { $statusCode += $errorCount; }   # Set non-zero rc if we detected logger errors
  if ($statusCode && !$errorCount) { $errorCount++; }                # Increment error counter if logger didn't catch it

  # If we got a value >255, assume we were passed a wait call exit status and right shift by 8 to get the return code
  my $statusCodeSmall = $statusCode;
  if ($statusCode > 255) { $statusCodeSmall = $statusCode >> 8; }
  if ($statusCode > 0 && ($statusCodeSmall % 256) == 0) { $statusCodeSmall = 1; }

  # Generate an informative sign off message for the log
  my $signOffMsg = "$parentName Exiting with return code of $statusCodeSmall";
  $signOffMsg .= ($statusCode != $statusCodeSmall) ? ", wait return code of $statusCode. " : ". ";
  $signOffMsg .= "$errorCount error(s), ";
  $signOffMsg .= "$warningCount warning(s) reported.";

  if   ($statusCode) { $logger->error($signOffMsg); }    # If we had a bad return code, log an error
  else               { $logger->info($signOffMsg); }     # Else log the sign off message as info

  return $statusCodeSmall;
} ## end sub signOff
##--------------------------------------------------------------------------

1;
