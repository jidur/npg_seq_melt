use strict;
use warnings;
use Test::More tests => 19;
use Test::Exception;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use File::Copy;
use Data::Dumper;
use Carp;
use Cwd;
use Log::Log4perl;
use IO::File;
use File::Basename qw/ basename /;

##################################################################################################
####export PATH=./bin:/opt/renci/icommands/bin:/software/solexa/pkg/baton/0.16.4-7-g322041b-v4/bin:/software/solexa/pkg/tears/04.10.16-v4:/nfs/users/nfs_a/aw7/scratch/aw7/biorods/bin:/software/npg/20161109/bin:${PATH}
####export PERL5LIB=./lib:/software/npg/20161109/lib/perl5:${PERL5LIB}
####export IRODS_ENVIRONMENT_FILE='/nfs/users/nfs_j/$ENV{USER}/.irods/irods_environment.json_sanger1_dev_nonkrb'
####export WTSI_NPG_MELT_iRODS_Test_irodsEnvFile='/nfs/users/nfs_j/$ENV{USER}/.irods/irods_environment.json_sanger1_dev_nonkrb'
#### versions specific to iRODS 4.1.9 :
#### /nfs/users/nfs_a/aw7/scratch/aw7/biorods/bin   /software/solexa/pkg/tears/04.10.16-v4
#### /software/solexa/pkg/baton/0.16.4-7-g322041b-v4/bin (build with 4.1.9 iRODS libraries, host name fixed)
#### TEST_AUTHOR=1 prove -vl t/TEST_10-melt-merge-library-run.t

####irm -r /seq-dev/npg/19900  if failed run has not cleaned up
##################################################################################################

use WTSI::NPG::iRODS;
use npg_tracking::glossary::composition::component::illumina;

use_ok('npg_seq_melt::merge::library');

my $correct_host;
my $dev_hostname = q[irods-sanger1-dev];
my $seen_hostname = qx(uname -n);
chomp($seen_hostname);
if ($dev_hostname eq $seen_hostname){ $correct_host=1}

#my $IRODS_WRITE_PATH = q[/seq-dev/npg/merged/];
#my $IRODS_ROOT       = q[/seq-dev/npg/];
#my $IRODS_PREFIX     = q[irods-r2-dev];
my $IRODS_WRITE_PATH = qq[/Sanger1-dev/home/$ENV{USER}/npg/merged/];
my $IRODS_ROOT        = qq[/Sanger1-dev/home/$ENV{USER}/npg/];
my $IRODS_PREFIX     = q[irods-sanger1-dev];

##set to dev iRODS
my $env_file = $ENV{'WTSI_NPG_MELT_iRODS_Test_irodsEnvFile'} || q{};

$ENV{TEST_DIR} = q(t/data);
my %env_copy = %ENV;
   $env_copy{'irodsEnvFile'} = $env_file || 'DUMMY_VALUE';
local %ENV = %env_copy;

Log::Log4perl::init_once('./t/log4perl_test.conf');
my $logger = Log::Log4perl->get_logger('dnap');
my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  logger => $logger);



{

  my $tempdir = tempdir( CLEANUP => 1);

  ## copy t/data/references/phix_unsnipped_short_no_N.fa to $tempdir/references/PhiX/Sanger-SNPs/all/fasta/phix_unsnipped_short_no_N.fa
  my $phix       = q[references/phix_unsnipped_short_no_N.fa];
  my $fasta      = join q[/],$ENV{TEST_DIR},$phix;
  my $copy_fasta = join q[/],$tempdir,$phix;
  make_path(join q[/],$tempdir,q[references]);
  make_path(join q[/],$tempdir,q[input]);
  copy($fasta,$copy_fasta) or carp "Copy failed: $!";
  print $fasta," ",$copy_fasta,"\n";

  ###load single library crams to dev irods + required meta data 

my $sample_id = '2092238';
my $library_id = '16477382';
my $Hr = {};
my @runs = (19900,19901,19902,19904);
  SKIP: {

    if ($env_copy{'irodsEnvFile'} && $env_copy{'irodsEnvFile'} ne 'DUMMY_VALUE'){
       if ($correct_host){
            foreach my $run (@runs){
               my $cram = $ENV{TEST_DIR} .q[/crams/].$run . q[_8#12.cram]; 
               my $seqchksum = $ENV{TEST_DIR} .q[/seqchksum/].$run . q[_8#12.seqchksum]; 
               $Hr = {
                     'irods'      => $irods,
                     'id_run'     => $run,
                     'cram'       => $cram,
                     'library_id' => $library_id,
                     'sample_id'  => $sample_id,
                     'seqchksum'  => $seqchksum,
                     };
                add_irods_data($Hr);
          }
       }
       else { skip qq[Host should be $dev_hostname],17  }
    }
    else { skip q[Environment variable WTSI_NPG_MELT_iRODS_Test_irodsEnvFile not set],17  }

   }

###e.g.
### /software/npg/20160516/bin/npg_library_merge --rpt_list '19915:4:6;19925:4:6;19941:4:6;19942:4:6;19945:4
###:6;19946:4:6' --library_id 16474932 --sample_id 2060841 --sample_name SC_WES_INT5915937 --sample_common_
###name 'Homo Sapien' --sample_accession_number EGAN00001391018 --study_id 3765 --study_name 'IHTP_WGS_INTE
###RVAL Cohort (15x)' --study_title 'Whole Genome Sequencing of INTERVAL' --study_accession_number EGAS0000
###1001355 --aligned 1 --lims_id SQSCP --instrument_type HiSeqX --run_type paired310 --chemistry CCXX  --sa
###mtools_executable  samtools1   --run_dir  /nfs/sf46/ILorHSany_sf46/library_merging/study_3765_library_me
###rging   --use_irods

##/nfs/sf46/ILorHSany_sf46/library_merging/study_3765_library_merging/log/cram_merge_2016-06-27T22:56:16_803585

##@SQ	SN:phix	LN:5386	M5:c3f78539481dde3e7fb732c446d42d93	UR:/lustre/scratch110/srpipe/references/PhiX/Sanger-S
##NPs/all/fasta/phix_unsnipped_short_no_N.fa

  my $sample_merge = npg_seq_melt::merge::library->new(
   rpt_list                => '19900:8:12;19901:8:12;19902:8:12;19904:8:12',
   sample_id               =>  $sample_id, 
   sample_name             => 'SC_WES_INT5948829',
   sample_common_name      => 'Homo Sapiens', #PhiX in test data    Enterobacteria phage phiX174 , PhiX174,PhiX175,PhiX176
   library_id              =>  $library_id, 
   library_type            =>  'Standard', 
   instrument_type         => 'HiSeqX',
   study_id                => '3765',
   study_name              => 'IHTP_WGS_INTERVAL Cohort (15x)',
   study_title             => 'Whole Genome Sequencing of INTERVAL',
   study_accession_number  => 'EGAS00001001355',
   run_type                =>  'paired310',
   chemistry               =>  'CCXX', 
   run_dir                 =>  $tempdir,
   aligned                 =>  1,
   irods                   =>  $irods, 
   lims_id                => 'SQSCP',
   sample_acc_check        =>  0,  #--nosample_acc_check
   reference_genome_path   => $copy_fasta,
   default_root_dir        => $IRODS_WRITE_PATH,
   irods_root              => $IRODS_ROOT, # standard_paths uses irods_root + id_run + cramfile
   _paths2merge           =>  ["${IRODS_ROOT}19900/19900_8#12.cram",
                               "${IRODS_ROOT}19901/19901_8#12.cram",
                               "${IRODS_ROOT}19902/19902_8#12.cram",
                               "${IRODS_ROOT}19904/19904_8#12.cram",
                              ],
  );

#TODO get error : NPG tracking reports run 19900 no longer on staging at reader srpipe::runfolder::runfolder_path  , defined in npg_tracking/lib/npg_tracking/illumina/run/folder/location.pm  if _paths2merge not given directly 


SKIP: {
    if ($env_copy{'irodsEnvFile'} && $env_copy{'irodsEnvFile'} ne 'DUMMY_VALUE'){
       if ($correct_host){
           is ($sample_merge->process(),undef, "cram merged and files/meta data added to iRODS"); #do_merge,  load_to_irods
       }
       else { skip qq[Host should be $dev_hostname],1  }
    }
    else { skip q[Environment variable WTSI_NPG_MELT_iRODS_Test_irodsEnvFile not set],1  }


chdir $tempdir;

is (-e $sample_merge->{'merge_dir'}.q[/status/merge_completed], 1, 'status/merge_completed file is present');

my $irods_merged_dir = $sample_merge->{'collection'} ;
is ($irods_merged_dir,$IRODS_WRITE_PATH.q[16477382.CCXX.paired310.9d1b3147e4],q[Collection name unchanged]);

my $expected_output_files = expected_output_files($irods_merged_dir);

foreach my $file (keys %{$expected_output_files}){

       my @irods_meta = $irods->get_object_meta($file);

     if ($file =~ /cram$/){
          delete $irods_meta[13]; #md5
          my $expected_cram_meta = cram_meta($tempdir);
          my $res = is_deeply(\@irods_meta,$expected_cram_meta,'cram meta data matches expected');
          if (!$res){
             carp "RECEIVED: ".Dumper(@irods_meta);
             carp "EXPECTED: ".Dumper($expected_cram_meta);
          }
      }
      else { #check types are the same 
       is ($irods_meta[4]->{value},$expected_output_files->{$file}->{type},"ok iRODS file type $expected_output_files->{$file}->{type}");
      }
}

my @coll_list = $irods->list_collection($irods_merged_dir,1);
my $expected_coll_list = expected_collection_list($irods_merged_dir);
my $coll_result = is_deeply($coll_list[0], @$expected_coll_list, 'irods merged collection list as expected');

  if (!$coll_result) {
    carp "RECEIVED: ".Dumper($coll_list[0]);
    carp "EXPECTED: ".Dumper(@$expected_coll_list);
  }


my $expected = expected_library_object($tempdir);
my $result = is_deeply($sample_merge, $expected, 'irods data to add as expected');

  if (!$result) {
    carp "RECEIVED: ".Dumper($sample_merge);
    carp "EXPECTED: ".Dumper($expected);
  }


## Remove temporary collections
  foreach my $run (@runs){
    my $tmp_coll = $IRODS_ROOT.$run; 
    $irods->remove_collection($tmp_coll) if ($env_copy{'irodsEnvFile'} && $env_copy{'irodsEnvFile'} ne 'DUMMY_VALUE');
  }

  #/seq-dev/npg/merged/16477382.CCXX.paired310.9d1b3147e4
  my $merged_coll = $IRODS_WRITE_PATH.$sample_merge->sample_merged_name;
   $irods->remove_collection($merged_coll) if ($env_copy{'irodsEnvFile'} && $env_copy{'irodsEnvFile'} ne 'DUMMY_VALUE'); 
 }

chdir $tempdir;

}
 

sub add_irods_data {
    my $Hr = shift;
    my $irods              = $Hr->{irods};
    my $coll_name          = $Hr->{id_run};
    my $cram_filename      = $Hr->{cram};
    my $seqchksum_filename = $Hr->{seqchksum};
    my $sample_id          = $Hr->{sample_id};
    my $library_id         = $Hr->{library_id};

my $irods_tmp_coll = $irods->add_collection(qq[$IRODS_ROOT/$coll_name]);
my $irods_cram_path = $irods_tmp_coll.q[/].basename($cram_filename);
   $irods->add_object($cram_filename,$irods_cram_path);

##add meta data
   $irods->add_object_avu($irods_cram_path,q[type],q[cram]);
   ##needed for _check_cram_header :  sample_id,  library_id
   $irods->add_object_avu($irods_cram_path,q[sample_id],$sample_id);
   $irods->add_object_avu($irods_cram_path,q[library_id],$library_id);

my $irods_seqchksum_path = $irods_tmp_coll.q[/].basename($seqchksum_filename);
   $irods->add_object($seqchksum_filename,$irods_seqchksum_path);

}

sub expected_library_object {
  my $tempdir = shift; 
  
  my $data = {};
  my $composition_digest = q[8db7cd4af68e9f2825e3e1274fb1aee0bd62da1defbed30b5f334880238779ac];

  $data= bless( {
     'verbose'                 => 0,
     'chemistry'               => 'CCXX',
     'library_id'              => '16477382',
     'sample_acc_check'        => 0,
     'study_title'             => 'Whole Genome Sequencing of INTERVAL',
     '_tar_log_files'          => 'library_merge_logs.tgz',
     'merge_dir'               =>  qq[$tempdir/$composition_digest],
     'irods_root'              =>  $IRODS_ROOT,
     'run_type'                => 'paired310',
     'default_root_dir'        =>  $IRODS_WRITE_PATH,
     'minimum_component_count' => 6,
     'sample_common_name'      => 'Homo Sapiens',
     'merged_qc_dir'           => qq[$tempdir/$composition_digest/outdata/qc/],
     'study_accession_number'  => 'EGAS00001001355',
     'original_seqchksum_dir'  => qq[$tempdir/$composition_digest/input],
     'study_name'              => 'IHTP_WGS_INTERVAL Cohort (15x)',
     '_sample_merged_name'     => '16477382.CCXX.paired310.9d1b3147e4',
     'sample_id'               => '2092238',
     'lims_id'                 => 'SQSCP',
     'mkdir_flag'              => 0,
     'samtools_executable'     => 'samtools1',
     'random_replicate'        => 0,
     'study_id'                => '3765',
     'vtlib'                   => '$(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/',
     'collection'              => $IRODS_WRITE_PATH.q[16477382.CCXX.paired310.9d1b3147e4],
     'local'                   => 0,
     '_runfolder_location'     => [],
     '_paths2merge' => [
          q[irods://].$IRODS_PREFIX.q[.internal.sanger.ac.uk].$IRODS_ROOT.q[19900/19900_8#12.cram],
          q[irods://].$IRODS_PREFIX.q[.internal.sanger.ac.uk].$IRODS_ROOT.q[19901/19901_8#12.cram],
          q[irods://].$IRODS_PREFIX.q[.internal.sanger.ac.uk].$IRODS_ROOT.q[19902/19902_8#12.cram],
          q[irods://].$IRODS_PREFIX.q[.internal.sanger.ac.uk].$IRODS_ROOT.q[19904/19904_8#12.cram]
                                   ],
     'reference_genome_path'   => qq[$tempdir/references/phix_unsnipped_short_no_N.fa],
     'sample_name'             => 'SC_WES_INT5948829',
     'composition'             => bless( {
                                           'components' => [
                                                             bless( {
                                                                      'tag_index' => '12',
                                                                      'position' => '8',
                                                                      'id_run' => '19900'
                                                                    }, 'npg_tracking::glossary::composition::component::illumina' ),
                                                             bless( {
                                                                      'tag_index' => '12',
                                                                      'position' => '8',
                                                                      'id_run' => '19901'
                                                                    }, 'npg_tracking::glossary::composition::component::illumina' ),
                                                             bless( {
                                                                      'tag_index' => '12',
                                                                      'position' => '8',
                                                                      'id_run' => '19902'
                                                                    }, 'npg_tracking::glossary::composition::component::illumina' ),
                                                             bless( {
                                                                      'tag_index' => '12',
                                                                      'position' => '8',
                                                                      'id_run' => '19904'
                                                                    }, 'npg_tracking::glossary::composition::component::illumina' ),
                                                           ]
                                         }, 'npg_tracking::glossary::composition' ),
     'rpt_list'                 => q[19900:8:12;19901:8:12;19902:8:12;19904:8:12],
     'library_type'             => 'Standard',
     'remove_outdata'           => 0,
     'log_file'                 => 'no log file',
     'aligned'                  => 1,
     'instrument_type'          => 'HiSeqX',
     'run_dir'                  => qq[$tempdir]
     }, 'npg_seq_melt::merge::library');


  return($data);
}

sub expected_output_files {

  my $irods_merged_dir = shift; 
 
  my $irods_files = {};
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4.cram]}                  = {'type' => 'cram' };
  #add other cram imeta
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4.cram.crai]}             = {'type' => 'crai' };
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4.flagstat]}              = { 'type' => 'flagstat' };
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4.seqchksum]}             = { 'type' => 'seqchksum' };
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4_F0xB00.stats]}          = { 'type' => 'stats' };
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4_F0x900.stats]}          = { 'type' => 'stats' };
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4.sha512primesums512.seqchksum]} = { 'type' => 'seqchksum'};
   $irods_files->{qq[$irods_merged_dir/16477382.CCXX.paired310.9d1b3147e4.markdups_metrics.txt]}  = {'type' => 'txt'};
   $irods_files->{qq[$irods_merged_dir/qc/16477382.CCXX.paired310.9d1b3147e4.bam_flagstats.json]} = {'type' => 'json'};
   $irods_files->{qq[$irods_merged_dir/library_merge_logs.tgz]}                                   = { 'type' => 'tgz' };
   $irods_files->{qq[$irods_merged_dir/qc/16477382.CCXX.paired310.9d1b3147e4.sequence_summary.json]} = {'type' => 'json'};
   $irods_files->{qq[$irods_merged_dir/qc/16477382.CCXX.paired310.9d1b3147e4_F0x900.samtools_stats.json]} = {'type' => 'json'};
   $irods_files->{qq[$irods_merged_dir/qc/16477382.CCXX.paired310.9d1b3147e4_F0xB00.samtools_stats.json]} = {'type' => 'json'};
  
  
  
return($irods_files);
}

sub expected_collection_list {
    my $irods_merged_dir = shift;
    my $expected_files = expected_output_files($irods_merged_dir);
    my @fn;
    push @fn, sort keys %$expected_files ;
return [ [@fn] ];
}

sub cram_meta {
    my $tempdir = shift;

  my @meta = ();

  @meta = (
        {
          'attribute' => 'alignment',
          'value' => 1
        },
        {
          'attribute' => 'chemistry',
          'value' => 'CCXX'
        },
        {
          'attribute' => 'component',
          'value' => '{"id_run":19900,"position":8,"tag_index":12}'
        },
        {
          'attribute' => 'component',
          'value' => '{"id_run":19901,"position":8,"tag_index":12}'
        },
        {
          'attribute' => 'component',
          'value' => '{"id_run":19902,"position":8,"tag_index":12}'
        }, 
        {
          'attribute' => 'component',
          'value' => '{"id_run":19904,"position":8,"tag_index":12}'
        },
        {
          'attribute' => 'composition',
          'value' => '{"components":[{"id_run":19900,"position":8,"tag_index":12},{"id_run":19901,"position":8,"tag_index":12},{"id_run":19902,"position":8,"tag_index":12},{"id_run":19904,"position":8,"tag_index":12}]}'
        },
        {
          'attribute' => 'composition_id',
          'value' => '8db7cd4af68e9f2825e3e1274fb1aee0bd62da1defbed30b5f334880238779ac'
        },
        {
          'attribute' => 'instrument_type',
          'value' => 'HiSeqX'
        },
        {
           'attribute' => 'is_paired_read',
           'value' => 1
         },
         {
           'attribute' => 'library_id',
           'value' => 16477382
         },
         {
           'attribute' => 'library_type',
           'value' => 'Standard'
         },
         {
           'attribute' => 'manual_qc',
           'value' => 1
         },
         undef,
         {
           'attribute' => 'reference',
           'value' => $tempdir.q[/references/phix_unsnipped_short_no_N.fa]
         },
         {
           'attribute' => 'run_type',
           'value' => 'paired310'
         },
         {
           'attribute' => 'sample',
           'value' => 'SC_WES_INT5948829'
         },
         {
           'attribute' => 'sample_common_name',
           'value' => 'Homo Sapiens'
         },
         {
           'attribute' => 'sample_id',
           'value' => 2092238
         },
         {
           'attribute' => 'study',
           'value' => 'IHTP_WGS_INTERVAL Cohort (15x)'
         },
         {
           'attribute' => 'study_accession_number',
           'value' => 'EGAS00001001355'
         },
         {
           'attribute' => 'study_id',
           'value' => 3765
         },
         {
           'attribute' => 'study_title',
           'value' => 'Whole Genome Sequencing of INTERVAL'
         },
         {
           'attribute' => 'target',
           'value' => 'library'
         },
         {
           'attribute' => 'total_reads',
           'value' => 14602
         },
         {
           'attribute' => 'type',
           'value' => 'cram'
         }

  );

  return \@meta;

}


1;
__END__
