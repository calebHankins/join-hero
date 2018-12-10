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
no if $] >= 5.017011, warnings => 'experimental::smartmatch';    # Suppress smartmatch warnings

##--------------------------------------------------------------------------
# Version info
our $VERSION = '0.0.1';
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
        $pkComponents->{$pkName}->{'pkType'}    = $pkType;
        $pkComponents->{$pkName}->{'fieldList'} = $fieldList;

        # Split and store field names as array elements
        @{$pkComponents->{$pkName}->{'fields'}} = split(',', $fieldList);
      } ## end if ($table and $pkName...)
    } ## end if ($pk =~ /$pkComponentsRegEx/gms)
  } ## end for my $pk (@keyDDL)

  if ($verbose) { $logger->info("$subName Primary and unique key components:\n" . Dumper($pkComponents)); }

  # Munge our FKs into components that we can use
  my $fkComponents = {};    # Hash ref to hold our broken out component parts
  for my $fk (@keyDDL) {
    if ($fk =~ /$fkComponentsRegEx/gms) {
      my $toTable       = $1;
      my $toFieldList   = $3;
      my $fkName        = $2;
      my $fromTable     = $4;
      my $fromFieldList = $5;

      # Store components if we have all the information that we will need
      if ($toTable and $toFieldList and $fkName and $fromTable and $fromFieldList) {

        # Remove any whitespace characters from the field lists
        $fromFieldList =~ s/\s+//g;
        $toFieldList =~ s/\s+//g;

        # Uppercase field lists
        $fromFieldList = uc($fromFieldList);
        $toFieldList   = uc($toFieldList);

        # Clean up names
        $fromTable = getCleanedObjectName($fromTable);
        $toTable   = getCleanedObjectName($toTable);

        # Save components
        $fkComponents->{$fkName}->{'fkName'}        = $fkName;
        $fkComponents->{$fkName}->{'fromTable'}     = getTableName($fromTable);
        $fkComponents->{$fkName}->{'fromFieldList'} = $fromFieldList;
        $fkComponents->{$fkName}->{'toTable'}       = getTableName($toTable);
        $fkComponents->{$fkName}->{'toFieldList'}   = $toFieldList;

        # Split and store field names as array elements
        @{$fkComponents->{$fkName}->{'fromFields'}} = split(',', $fromFieldList);
        @{$fkComponents->{$fkName}->{'toFields'}}   = split(',', $toFieldList);

        # Munge and set schema names using the table prefix
        my $fromSchema = getSchemaName($fromTable, \@supportedMarts);
        my $toSchema   = getSchemaName($toTable,   \@supportedMarts);

        # If we got one schema but not the other, set the empty one using the populated one
        if ($fromSchema && !$toSchema)   { $toSchema   = $fromSchema; }
        if ($toSchema   && !$fromSchema) { $fromSchema = $toSchema; }

        # Save munged schema components
        $fkComponents->{$fkName}->{'fromSchema'} = $fromSchema;
        $fkComponents->{$fkName}->{'toSchema'}   = $toSchema;
      } ## end if ($toTable and $toFieldList...)
    } ## end if ($fk =~ /$fkComponentsRegEx/gms)
  } ## end for my $fk (@keyDDL)

  if ($verbose) { $logger->info("$subName Foreign keys components:\n" . Dumper($fkComponents)); }

  return ($pkComponents, $fkComponents);
} ## end sub getKeyComponents
##---------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Use component hash refs to generate update SQL
sub getOutputSQL {
  my ($getOutputSQLParams) = @_;
  my $subName = (caller(0))[3];
  my $outputSQL;
  my $uncommittedTransactions = 0;

  # Alias our params for easier use
  my $pkComponents    = $getOutputSQLParams->{pkComponents};
  my $fkComponents    = $getOutputSQLParams->{fkComponents};
  my $commitThreshold = $getOutputSQLParams->{commitThreshold};

  for my $key (sort keys %{$fkComponents}) {
    my $joinSQL = getJoinSQL($key, $getOutputSQLParams);
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

  $outputSQL .= "\ncommit;\n";

  return $outputSQL;
} ## end sub getOutputSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Use component hash refs to generate merge SQL
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
  my @supportedTypes           = @{$getJoinSQLParams->{supportedTypes}};
  my @supportedMarts           = @{$getJoinSQLParams->{supportedMarts}};
  my $allowUnknownSchema       = $getJoinSQLParams->{allowUnknownSchema};

  if ($verbose) { $logger->info("$subName Processing:$fkComponents->{$fkKey}->{fkName}...\n"); }
  for my $type (@supportedTypes) {
    if ($verbose) { $logger->info("$subName Processing:$fkComponents->{$fkKey}->{fkName} for type $type...\n"); }

    # App specific init
    my $fromSchema;
    my $fromTable;
    my $toSchema;
    my $toTable;
    if ($type eq 'ADOBE') {    # Adobe joins are backwards, flip them
      $fromSchema = $fkComponents->{$fkKey}->{'toSchema'};
      $fromTable  = $fkComponents->{$fkKey}->{'toTable'};
      $toSchema   = $fkComponents->{$fkKey}->{'fromSchema'};
      $toTable    = $fkComponents->{$fkKey}->{'fromTable'};
    } ## end if ($type eq 'ADOBE')
    else {
      $fromSchema = $fkComponents->{$fkKey}->{'fromSchema'};
      $fromTable  = $fkComponents->{$fkKey}->{'fromTable'};
      $toSchema   = $fkComponents->{$fkKey}->{'toSchema'};
      $toTable    = $fkComponents->{$fkKey}->{'toTable'};
    } ## end else [ if ($type eq 'ADOBE') ]

    # Check for valid table names, leave early if invalid
    if ($type ne 'ADOBE') {
      if ($toTable eq 'RECIPIENT' || $fromTable eq 'RECIPIENT') {
        if ($verbose) {
          $logger->info(
                    "$subName Non-Adobe type ($type) targeting a RECIPIENT table ($toTable to $fromTable), skipping\n");
        }
        next;
      } ## end if ($toTable eq 'RECIPIENT'...)
    } ## end if ($type ne 'ADOBE')

    # Validate schema, set default if empty
    if (!defined($toSchema) || !defined($fromSchema)) {
      if ($verbose) { $logger->info("$subName Setting default schema\n"); }
      $toSchema   = 'UNKNOWN';
      $fromSchema = 'UNKNOWN';
      if (!$allowUnknownSchema) { return; }    # Leave early unless we allow UNKNOWN schema
    } ## end if (!defined($toSchema...))

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
                    "$subName Processing:$fkComponents->{$fkKey}->{fkName} for type $type and toField $fkToField...\n");
      }
      my $fkFromField  = @{$fkComponents->{$fkKey}->{'fromFields'}}[$i];    # Grab the matching from field
      my $fieldJoinOrd = $i + 1;

      # Determine the to and from fields
      my $fromField;
      my $toField;
      if ($type eq 'ADOBE') {                                               # Adobe joins are backwards
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
          'AUTO-GENERATED BY Unregistered JoinHero 2 using $fkKey' as NOTES,
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
    my $direction = $type eq 'ADOBE' ? 'from' : 'to';
    my $cardinality = getJoinCardinality($pkComponents, $fkComponents->{$fkKey}, $direction);

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
          'AUTO-GENERATED BY Unregistered JoinHero 2 using $fkKey' as NOTES,
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

  } ## end for my $type (@supportedTypes)

  return $outputSQL;
} ## end sub getJoinSQL
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Calculate a given join's cardinality
sub getJoinCardinality {
  my ($pkComponents, $join, $direction) = @_;
  my $subName    = (caller(0))[3];
  my @joinFields = sort($join->{"${direction}Fields"});
  my $cardinality = 'MANY';    # Default cardinality to MANY, we'll override later if we have a key match

  # If the join's toFields match a unique key for toTable, flag that join as 1-1
  for my $pkKey (sort keys %{$pkComponents}) {
    if ($join->{"${direction}Table"} eq $pkComponents->{$pkKey}->{'table'}) {
      my @pkFields = sort($pkComponents->{$pkKey}->{'fields'});
      if (@joinFields ~~ @pkFields) {
        $cardinality = 'ONE';
        if ($verbose) { $logger->info("$subName Setting cardinality = '$cardinality'\n"); }
        last;                  # If we found a key match, leave early
      }
    } ## end if ($join->{"${direction}Table"...})
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

  # If we got a dot in the object name, take the secod half, else take the original
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
# Get unique array curtesy of perlfaq4
sub getUniqArray {
  my (@array) = @_;
  my %seen = ();
  my @unique = grep { !$seen{$_}++ } @array;
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
