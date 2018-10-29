#!/usr/bin/perl
#/*#########################################################################################
#                       (C) Copyright Acxiom Corporation 2018
#                               All Rights Reserved.
############################################################################################
#
# Script: join_hero.pl
# Author: Caleb Hankins - chanki
# Date:   2018-03-27
#
# Purpose: Transform DDL that describes keys (foreign, primary and unique)
# into join metadata that can be ingested by various and sundry downstream processes
# to link relational tables and views
#
############################################################################################
# MODIFICATION HISTORY
##-----------------------------------------------------------------------------------------
# DATE        PROGRAMMER                   DESCRIPTION
##-----------------------------------------------------------------------------------------
# 2018-03-27  Caleb Hankins - chanki       Initial Copy
###########################################################################################*/

use strict;
use warnings;
use IO::Handle;      # Supply object methods for I/O handles
use Getopt::Long;    # Extended processing of command line options
use Pod::Usage;      # Print a usage message from embedded pod documentation

use Data::Dumper;
use File::Path qw(make_path);

# turn on auto-flush / hot pipes
STDOUT->autoflush(1);
STDERR->autoflush(1);

my $inputFilepath                     = '';
my $updateExisting                    = '';
my $deleteExisting                    = '';
my $types                             = '';
my $marts                             = '';
my @supportedTypes                    = ('ADOBE', 'REDPOINT', 'IBM');
my @supportedMarts                    = ('CMP_DM', 'ITA', 'PA');
my $outputFilepath                    = 'join_us.sql';
my $martTableJoinTableName            = 'MART_TABLE_JOIN';
my $martTableJoinCardinalityTableName = 'MART_TABLE_JOIN_CARDINALITY';
my $commitThreshold                   = 1000;
my $coreFlg                           = 'Y';
my $testMode                          = '';
my $verbose                           = '';

my $rc = GetOptions(
  'i|file|inputFilepath=s'              => \$inputFilepath,
  'o|out|outputFilepath=s'              => \$outputFilepath,
  'types=s'                             => \$types,
  'marts=s'                             => \$marts,
  'u|updateExisting'                    => \$updateExisting,
  'd|deleteExisting'                    => \$deleteExisting,
  'martTableJoinTableName=s'            => \$martTableJoinTableName,
  'martTableJoinCardinalityTableName=s' => \$martTableJoinCardinalityTableName,
  'commitThreshold=i'                   => \$commitThreshold,
  'coreFlg=s'                           => \$coreFlg,

  't|testMode' => \$testMode,
  'v|verbose'  => \$verbose,

  #pod2usage variables
  'help' => sub { pod2usage(1); },
  'man'  => sub { pod2usage(-exitstatus => 0, -verbose => 2); }
);

sanityCheckOptions();

# Load up DDL source file
my $inputFileContents = openAndLoadFile($inputFilepath);

# Parse DDL Source file into usable components
my ($pk, $fk) = getKeyComponents($inputFileContents);

# Use our component list to construct some oracle merge statements
my $outputFileContents = getOutputSQL($pk, $fk);

# Create file containing SQL statements that can be used to update join metadata
createExportFile($outputFileContents, $outputFilepath);

exit;

##---------------------------------------------------------------------------
# Give script options the ol' sanity check
sub sanityCheckOptions {
  my $subName  = (caller(0))[3];
  my $errorCnt = 0;

  if ($types) { @supportedTypes = split(',', $types); }
  if ($marts) { @supportedMarts = split(',', $marts); }
  if ('CMP'    ~~ @supportedMarts) { push(@supportedMarts, 'CMP_DM'); }    # Add the long name if short is present
  if ('CMP_DM' ~~ @supportedMarts) { push(@supportedMarts, 'CMP'); }       # Add the short name if long is present
  @supportedMarts = getUniqArray(@supportedMarts);

  print("$subName Supported target application types: [" . join(',', @supportedTypes) . "]\n");
  print("$subName Supported mart prefixes: [" . join(',', @supportedMarts) . "]\n");
  print("$subName Processing '$inputFilepath', wish me luck!\n");

  $inputFilepath = glob($inputFilepath);
  $errorCnt += checkRequiredParm($inputFilepath, 'inputFilepath');
  $outputFilepath = glob($outputFilepath);
  $errorCnt += checkRequiredParm($outputFilepath, 'outputFilepath');

  # Check for errors before starting processing
  if ($errorCnt > 0) {

    # Print informational message to standard output
    print(  "$subName There were ["
          . $errorCnt
          . "] error messages detected while sanity checking options. Script is halting.");

    # Exit with a non-zero code and print usage
    pod2usage(10);
  } ## end if ($errorCnt > 0)

  return;
} ## end sub sanityCheckOptions
##---------------------------------------------------------------------------

##---------------------------------------------------------------------------
# Capture and save valuable components in the supplied DDL file
sub getKeyComponents {
  my ($rawDDL) = @_;
  my $subName = (caller(0))[3];

  # Describe what things look like
  my $validOracleObjectCharacterClasses = q{"a-zA-Z0-9_\$\@};                              # Oracle object
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
  if ($verbose) { print("$subName Parsing foreign keys using RegEx: $fkComponentsRegEx\n"); }

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
  if ($verbose) { print("$subName Parsing primary and unique keys using RegEx: $pkComponentsRegEx\n"); }

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

  if ($verbose) { print("$subName Primary and unique key components:\n" . Dumper($pkComponents)); }

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
        my $fromSchema = getSchemaName($fkComponents->{$fkName}->{'fromTable'});
        my $toSchema   = getSchemaName($fkComponents->{$fkName}->{'toTable'});

        # If we got one schema but not the other, set the empty one using the populated one
        if ($fromSchema && !$toSchema)   { $toSchema   = $fromSchema; }
        if ($toSchema   && !$fromSchema) { $fromSchema = $toSchema; }

        # Save munged schema components
        $fkComponents->{$fkName}->{'fromSchema'} = $fromSchema;
        $fkComponents->{$fkName}->{'toSchema'}   = $toSchema;
      } ## end if ($toTable and $toFieldList...)
    } ## end if ($fk =~ /$fkComponentsRegEx/gms)
  } ## end for my $fk (@keyDDL)

  if ($verbose) { print("$subName Foreign keys components:\n" . Dumper($fkComponents)); }

  return ($pkComponents, $fkComponents);
} ## end sub getKeyComponents
##---------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Use component hash refs to generate update SQL
sub getOutputSQL {
  my ($pkComponents, $fkComponents) = @_;
  my $subName = (caller(0))[3];
  my $outputSQL;
  my $uncommittedTransactions = 0;

  for my $key (sort keys %{$fkComponents}) {
    my $joinSQL = getJoinSQL($pkComponents, $fkComponents, $key);
    if ($joinSQL) {
      $outputSQL .= $joinSQL;
      $uncommittedTransactions += () = $joinSQL =~ /;/g;    # Count semicolons to determine transactions added
      if ($verbose) { print("$subName uncommittedTransactions: $uncommittedTransactions\n"); }
      if ($uncommittedTransactions > $commitThreshold) {    # If we've reached the threshold, commit and reset count
        print("$subName Reached transaction threshold, inserting commit\n");
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
  my ($pkComponents, $fkComponents, $fkKey) = @_;
  my $subName   = (caller(0))[3];
  my $outputSQL = '';

  if ($verbose) { print("$subName Processing:$fkComponents->{$fkKey}->{fkName}...\n"); }
  for my $type (@supportedTypes) {
    if ($verbose) { print("$subName Processing:$fkComponents->{$fkKey}->{fkName} for type $type...\n"); }

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
          print("$subName Non-Adobe type ($type) targeting a RECIPIENT table ($toTable to $fromTable), skipping\n");
        }
        next;
      } ## end if ($toTable eq 'RECIPIENT'...)
    } ## end if ($type ne 'ADOBE')

    # Validate schema, exit early if invalid
    if (!defined($toSchema) || !defined($fromSchema)) {
      if ($verbose) { print("$subName Missing 1 or more schema, skipping\n"); }
      next;
    }
    elsif ($toSchema ne $fromSchema) {    # Exit early if the schema don't match, no cross mart joins
      if ($verbose) {
        print("$subName Cross schema ($toSchema to $fromSchema), skipping\n");
      }
      next;
    } ## end elsif ($toSchema ne $fromSchema)

    # Generate MTJ record(s)
    if ($deleteExisting) {
      my $deleteSQLMartTableJoin = qq{
        DELETE FROM $martTableJoinTableName A
        WHERE 
          A.TYPE = '$type' AND
          A.FROM_SCHEMA = '$fromSchema' AND
          A.TO_SCHEMA = '$toSchema' AND
          A.FROM_TABLE = '$fromTable' AND
          A.TO_TABLE = '$toTable';\n};

      if ($verbose) { print("$subName Adding deleteSQLMartTableJoin:\n$deleteSQLMartTableJoin\n"); }

      $outputSQL .= $deleteSQLMartTableJoin;
    } ## end if ($deleteExisting)

    my $i = 0;    # Setup a loop counter to use for field indexing and numbering
    for my $fkToField (@{$fkComponents->{$fkKey}->{'toFields'}}) {
      if ($verbose) {
        print("$subName Processing:$fkComponents->{$fkKey}->{fkName} for type $type and toField $fkToField...\n");
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

      my $mergeSQLMartTableJoin = qq{
        MERGE INTO $martTableJoinTableName A USING
        (SELECT
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
          FROM DUAL) B
        ON (  A.TYPE = B.TYPE 
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
          A.NOTES = B.NOTES,
          A.CORE_FLG = B.CORE_FLG};
      } ## end if ($updateExisting)

      $mergeSQLMartTableJoin .= ";\n";

      if ($verbose) { print("$subName Adding mergeSQLMartTableJoin:\n$mergeSQLMartTableJoin\n"); }

      $outputSQL .= $mergeSQLMartTableJoin;

      $i++;    # Record that we added a join record
    } ## end for my $fkToField (@{$fkComponents...})

    # Add cardinality record
    my $direction = $type eq 'ADOBE' ? 'from' : 'to';
    my $cardinality = getJoinCardinality($pkComponents, $fkComponents->{$fkKey}, $direction);

    if ($deleteExisting) {
      my $deleteSQLMartTableJoinCardinality = qq{
          DELETE FROM $martTableJoinCardinalityTableName A
          WHERE 
            A.TYPE = '$type' AND
            A.FROM_SCHEMA = '$fromSchema' AND
            A.TO_SCHEMA = '$toSchema' AND
            A.FROM_TABLE = '$fromTable' AND
            A.TO_TABLE = '$toTable';\n};

      if ($verbose) {
        print("$subName Adding deleteSQLMartTableJoinCardinality:\n$deleteSQLMartTableJoinCardinality\n");
      }

      $outputSQL .= $deleteSQLMartTableJoinCardinality;
    } ## end if ($deleteExisting)

    my $mergeSQLMartTableJoinCardinality = qq{
      MERGE INTO $martTableJoinCardinalityTableName A USING
      (SELECT
        '$fromSchema' as FROM_SCHEMA,
        '$fromTable' as FROM_TABLE,
        '$toSchema' as TO_SCHEMA,
        '$toTable' as TO_TABLE,
        '$cardinality' as CARDINALITY,
        '$type' as TYPE,
        'AUTO-GENERATED BY Unregistered JoinHero 2 using $fkKey' as NOTES,
        '$coreFlg' as CORE_FLG
        FROM DUAL) B
      ON (
        A.TYPE = B.TYPE 
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
          A.NOTES = B.NOTES,
          A.CORE_FLG = B.CORE_FLG};
    } ## end if ($updateExisting)

    $mergeSQLMartTableJoinCardinality .= ";\n";

    if ($verbose) {
      print("$subName Adding mergeSQLMartTableJoinCardinality:\n$mergeSQLMartTableJoinCardinality\n");
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
        if ($verbose) { print("$subName Setting cardinality = '$cardinality'\n"); }
        last;                  # If we found a key match, leave early
      }
    } ## end if ($join->{"${direction}Table"...})
  } ## end for my $pkKey (sort keys...)

  return $cardinality;
} ## end sub getJoinCardinality
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Derive a table name, apply any digital transformation services needed for translation
sub getTableName {
  my ($objectName) = @_;
  my $subName = (caller(0))[3];

  # If we got a name wrapped in quotes, strip them off but preserve the casing
  if ($objectName =~ /"/gm) {
    $objectName = stripQuotes($objectName);
  }
  else {
    # Else uppercase and move on
    $objectName = uc($objectName);
  }

  # If we got some variant of recipient, just return recipient
  $objectName =~ /(RECIPIENT)/gms;
  if ($1 and $1 eq 'RECIPIENT') { $objectName = 'RECIPIENT'; }

  return $objectName;
} ## end sub getTableName
##--------------------------------------------------------------------------

##--------------------------------------------------------------------------
# Derive the schema name using the table name
# Use the first part of the table name up to the first underscore for the schema name
sub getSchemaName {
  my ($tableName) = @_;

  # Take the first token using underscore delimiter
  my $schemaName = ($tableName =~ /([a-zA-Z0-9]+?)_.*/)[0];

  # CMP prefixes are special and actually mean CMP_DM
  if (defined $schemaName) {
    if ($schemaName eq 'CMP') { $schemaName = 'CMP_DM'; }
  }

  # Check schema is in our whitelist. If not, unset the value
  if (!($schemaName ~~ @supportedMarts)) { $schemaName = undef; }

  return $schemaName;
} ## end sub getSchemaName
##--------------------------------------------------------------------------

########################### todo, replace these subs below with partnerApps.pm if the cross platform issues are solved
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
  open my $fileHandle, "<", $filename or print("$subName Could not open file '$filename' $!");

  # Read file handle data stream into our file variable
  while (defined(my $line = <$fileHandle>)) {
    $fileContents .= $line;
  }

  close($fileHandle);

  if (length($fileContents) <= 0) {
    print("$subName It appears that nothing was in [$filename] Please check file and see if it meets expectations.");
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
  print("$subName Could not make directory: $filepathParts->{'dirname'}: $@") if $@;

  print("$subName Attempting creation of file [$exportFileFullName]\n");
  open my $exportFile, q{>}, $exportFileFullName
    or print("$subName Could not open file: $exportFileFullName: $!\n")
    ;    # Open file, overwrite if exists, raise error if we run into trouble
  if (!$utfDisabled) { binmode($exportFile, ":encoding(UTF-8)") }
  print $exportFile $fileData;
  close($exportFile);
  print("$subName Success!\n");

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
  print("$subName Could not fileparse '$filename'. Error message from fileparse: '$@'") if $@;

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
    print("$parentName $errMsg\n");
    $errorCnt++;
  }

  return $errorCnt;
} ## end sub checkRequiredParm
##--------------------------------------------------------------------------

##---------------------------------------------------------------------------
# Podusage

__END__

=head1 AUTHOR

Caleb Hankins - chanki

=head1 NAME

join_hero.pl

=head1 SYNOPSIS

join_hero.pl - Transform DDL that describes keys (foreign, primary and unique) into join metadata that can be ingested by various and sundry downstream processes to link relational tables and views

 Options:
  'i|file|inputFilepath=s'              DDL File input file with key info [required]
  'o|out|outputFilepath=s'              Output filepath for metadata [optional]
  'types=s'                             Application type [optional]
  'marts=s'                             Data mart prefix [optional]
  'u|updateExisting'                    Update existing metadata [optional]
  'd|deleteExisting'                    Delete existing metadata [optional]
  'martTableJoinTableName=s'            Override for MTJ table name [optional]
  'martTableJoinCardinalityTableName=s' Override for MTJ Cardinality table nam[optional]
  'commitThreshold=i'                   Statements to execute before issuing a commit [optional]
  'coreFlg=s'                           Set metadata as 'Core' [optional]

  't|testMode'
  'v|verbose'
  
  --help  Print brief help information.
  --man   Read the manual, includes examples.

=head1 EXAMPLES

# Generate a full_insert.sql file containing SQL commands to update metadata
perl join_hero.pl -i './Export/17.4 Export - FKs and PKs.ddl' -o './update_sql/full_insert.sql' -v > ./logs/full_insert.log

# Override target tables to research tables, include delete flag for cleanup
perl join_hero.pl -i '.\ddl\Campaign_Data_Mart.ddl' -o './update_sql/full_update.sql' > ./logs/full_update.log --martTableJoinTableName 'TEMP_MERGED_MTJ' --martTableJoinCardinalityTableName 'TEMP_MERGED_MTJ_CARD' -d
  
=cut
##---------------------------------------------------------------------------
