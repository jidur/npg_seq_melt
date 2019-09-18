use strict;
use warnings;
use t::dbic_util;
use File::Copy;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use Carp;
use File::Slurp;
use Data::Dumper;


use Test::More tests => 6;

use_ok('npg_seq_melt::archive::top_up');


$ENV{TEST_DIR} = q(t/data);

my $dbic_util = t::dbic_util->new();
my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh_topup');

my $tempdir = tempdir( CLEANUP => 0);

{


# local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = qq[$tempdir/f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230.csv];

my $expected_cmd_file = join q[/],$ENV{TEST_DIR},q[wr],q[wr_archive_cmds.txt];
my $expected_cmd_file_copy = join q[/],$tempdir,q[copy_wr_archive_cmds.txt];
copy($expected_cmd_file,$expected_cmd_file_copy) or carp "Copy failed: $!";

my $ss = join q[/],$ENV{TEST_DIR},q[samplesheets],q[f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230.csv];
 my $ss_copy = join q[/],$tempdir,q[f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230.csv];
  copy($ss,$ss_copy) or carp "Copy failed: $!";


make_path(join q[/],$tempdir,q[configs]);

my $config_file = join q[/],$ENV{TEST_DIR},q[configs],q[product_release.yml];  
my $config_file_copy = join q[/],$tempdir,q[configs],q[product_release.yml];
copy($config_file,$config_file_copy) or carp "Copy failed: $!";

my $composition_path = join q[/],$tempdir,q[composition_path];
make_path($composition_path);
my $composition_json = join q[/],$ENV{TEST_DIR},q[48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95.composition.json];
my $composition_json_copy = join q[/],$tempdir,q[composition_path],q[48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95.composition.json];
copy($composition_json,$composition_json_copy) or carp "Copy failed: $!";
chdir $tempdir;


#### starting point of --composition_path
my $a = npg_seq_melt::archive::top_up->new(rt_ticket => q[12345],
                                           conf_path => qq[$tempdir/configs],
                                           composition_path => $composition_path,
                                           id_study_lims => q[5392],
                                           repository => $tempdir
                                           );

my $c = &expected_composition;

isa_ok($a->qc_schema,'npg_qc::Schema');


is_deeply($a->compositions,$c,q[npg_tracking::glossary::composition returned correctly with composition_path]);

$a->exists_in_eventsdb();

#### starting point of --rpt_list   
my $b = npg_seq_melt::archive::top_up->new(rt_ticket => q[12345],
                                           conf_path => qq[$tempdir/configs],
                                           rpt_list  => "27312:1:15;27312:2:15;27312:3:15;27312:4:15;28780:2:5",
                                           id_study_lims => q[5392],
                                           repository => $tempdir,
                                           commands_file => qq[$tempdir/wr_qc_db_cmd.txt],
                                           load_to_qc_database_only => 1
                                           );

is_deeply($b->compositions,$c,q[npg_tracking::glossary::composition returned correctly with rpt_list ]);


$b->make_commands();

### without load_to_qc_database_only


my $d = npg_seq_melt::archive::top_up->new(rt_ticket => q[12345],
                                           conf_path => qq[$tempdir/configs],
                                           rpt_list  => "27312:1:15;27312:2:15;27312:3:15;27312:4:15;28780:2:5",
                                           id_study_lims => q[5392],
                                           repository => $tempdir,
                                           commands_file => qq[$tempdir/wr_s3_mlwh_cmd.txt],
                                           );


$d->make_commands();

is ($d->composition_id,qq[48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95],q[composition id o.k.]);

my $expected_fh = IO::File->new("jq . -S $expected_cmd_file_copy |") or croak "Cannot open $expected_cmd_file_copy";
my @expected_wr_commands_str;
   while(<$expected_fh>){ push @expected_wr_commands_str,$_ }

my $fh = IO::File->new("jq . -S $tempdir/copy_wr_archive_cmds.txt |") or croak "Cannot open $tempdir/copy_wr_archive_cmds.txt";
my @wr_commands_str;
   while(<$fh>){ 
      #  s#conf_path=/tmp/\S+/configs#conf_path=/path_to/configs#; 
      #  s#/tmp/\S+/(references|geno_refset)#/my/$1#g;
        push @wr_commands_str,$_ ;
   }

print Dumper  @wr_commands_str;

is_deeply(\@wr_commands_str,\@expected_wr_commands_str,q[wr commands match expected]);


###rpt not in events db 

my $e = npg_seq_melt::archive::top_up->new(rt_ticket => q[12345],
                                           conf_path => qq[$tempdir/configs],
                                           rpt_list  => "11111:1:15;27312:2:15;27312:3:15;27312:4:15;28780:2:5",
                                           id_study_lims => q[5392],
                                           repository => $tempdir
                                           );
$e->exists_in_eventsdb();

}



sub expected_composition{
      return
      bless( {
                 'components' => [
                                   bless( {
                                            'position' => 1,
                                            'id_run' => 27312,
                                            'tag_index' => 15
                                          }, 'npg_tracking::glossary::composition::component::illumina' ),
                                   bless( {
                                            'id_run' => 27312,
                                            'position' => 2,
                                            'tag_index' => 15
                                          }, 'npg_tracking::glossary::composition::component::illumina' ),
                                   bless( {
                                            'tag_index' => 15,
                                            'id_run' => 27312,
                                            'position' => 3
                                          }, 'npg_tracking::glossary::composition::component::illumina' ),
                                   bless( {
                                            'tag_index' => 15,
                                            'id_run' => 27312,
                                            'position' => 4
                                          }, 'npg_tracking::glossary::composition::component::illumina' ),
                                   bless( {
                                            'tag_index' => 5,
                                            'id_run' => 28780,
                                            'position' => 2
                                          }, 'npg_tracking::glossary::composition::component::illumina' )
                                 ]
               }, 'npg_tracking::glossary::composition' );

}

1;

