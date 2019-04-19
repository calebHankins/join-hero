package JoinHero;

use 5.0100;
use strict;
use warnings;

use Module::Build;

my $build = Module::Build->new(
                     module_name   => 'JoinHero',
                     license       => 'apache',
                     dist_abstract => 'Oracle DDL parser for scraping PK/FK/Unique Key metadata describing table joins',
                     dist_author   => 'Caleb Hankins (caleb.hankins at acxiom.com)',
                     script_files  => 'script/join-hero',
                     release_status => 'unstable',
                     requires       => {
                                  'perl'          => '5.10.1',
                                  'Data::Dumper'  => '0',
                                  'File::Path'    => '0',
                                  'File::Glob'    => '0',
                                  'Log::Log4perl' => '0',
                                  'IO::Handle'    => '0',
                                  'Getopt::Long'  => '0',
                                  'Pod::Usage'    => '0',
                                  'Cwd'           => '0',
                                  'Graph'         => '0',
                     },
                     recommended_requires => {
                                              'English'                         => '0',
                                              'Test::Perl::Critic'              => '0',
                                              'Test::Code::TidyAll'             => '0',
                                              'Perl::Tidy'                      => '0',
                                              'Devel::Cover'                    => '0',
                                              'Devel::Cover::Report::Coveralls' => '0',
                     },
                     configure_requires => {'Module::Build' => '0'},
                     create_makefile_pl => 'traditional',
);

# Install any data files in /lib
$build->add_build_element('xml');
$build->add_build_element('json');
$build->add_build_element('dat');

$build->create_build_script;

1;
