package npg_seq_melt::archive::top_up;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Carp;
use English qw(-no_match_vars);
use Pod::Usage;
use IO::File;
use File::Slurp;
use JSON;
use DBI;
#use List::Util qw{all};

use npg_tracking::glossary::composition;


extends qw{npg_seq_melt::query::top_up};

with qw{
     npg_common::roles::log
};

our $VERSION = '0';
Readonly::Scalar my $WR_PRIORITY  => 51;
Readonly::Scalar my $MEMORY_4000M => q[4000M];


=head1 NAME

    npg_seq_melt::archive::top_up

=head1 SYNOPSIS

npg_seq_melt::archive::top_up->new( rt_ticket => 123445, rpt_list  => "27312:1:15;27312:2:15;27312:3:15;27312:4:15;28780:2:5", dry_run => 1)->run;


=head1 SUBROUTINES/METHODS

=head2 rt_ticket

For use with --label with npg_pipeline_post_qc_review rt$rt_ticket

=cut

has 'rt_ticket'     => ( isa           => 'Int',
                         is            => 'ro',
                         required      => 1,
                         documentation => q[RT ticket for batch processed],
);


=head2 wr_deployment

production or developmet

=cut

has 'wr_deployment'  => ( isa           => 'Str',
                          is            => 'ro',
                          default       => 'production',
                          documentation => q[ For use with wr --deployment option (production or development) ],
);

=head2 commands_file

File name to write wr commands to

=cut

has 'commands_file' => ( isa           => 'Str',
                         is            => 'ro',
                         default       => q[/tmp/wr_archive_cmds.txt],

                         documentation => 'File name to write wr commands to',
    );


=head2 wr_env

=cut


has 'wr_env'  => (isa  => 'Str',
                  is   => 'ro',
                  documentation => 'Environment to find relevant scripts and files. NPG_REPOSITORY_ROOT=, REF_PATH=,PATH=,PERL5LIB=',
);


=head2 supplier_sample 

=cut

has 'supplier_sample' => ( isa           => 'Str',
                           is            => 'rw',
                           documentation => 'Sequencescape supplier sample name',
    );


=head2 composition_id 

=cut

has 'composition_id' => ( isa           => 'Str',
                          is            => 'rw',
                          documentation => 'Composition id from npg_pipeline::product file_name_root',
    );


=head2 archive_path

Parent directory is from the component_cache_dir in the product_release.yml config file.
Used with npg_pipeline_post_qc_review.

=cut

has 'archive_path' => ( isa           => 'Str',
                        is            => 'rw',
                        documentation => 'Excluding digest e.g. /lustre/scratch113/merge_component_results/5392/48/bc/',
                        lazy_build    => 1,
    );
sub _build_archive_path {
    my $self = shift;
    my ($name) =join q[/],$self->_cache_name();
        $name =~ s/cache/results/sm;
    my $digest = $self->composition_id;
        $name =~ s/$digest//smx;
return($name);
}

=head2 log_dir

For npg_pipeline_post_qc_review

=cut

has 'log_dir' => ( isa           => 'Str',
                   is            => 'rw',
                   documentation => 'For npg_pipeline_post_qc_review e.g. /lustre/scratch113/tickets/rt668088/logs/48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95',
                   lazy_build   => 1
    );
sub _build_log_dir{ ##TODO - change hard coded path prefix
    my $self = shift;
    return(join q[/],q[/lustre/scratch113/tickets],(q[rt].$self->rt_ticket),$self->composition_id);
}


=head2 rpt_list

Semi-colon separated list of run:position or run:position:tag for the same sample
that define a composition for this merge. An optional attribute.

=cut

has 'rpt_list' => (
     isa           => q[Str],
     is            => q[ro],
     predicate     => q[has_rpt_list],
     );

=head2 composition_path

=cut

has 'composition_path' => (
                            isa           => q[Str],
                            is            => 'ro',
                            documentation => 'paths with composition.json file for top-up merged data',
                            predicate     => q[has_composition_path],
    );


=head2 compositions

=cut

has 'compositions' => (
                        isa               => q[npg_tracking::glossary::composition],
                        is                => q[rw],
                        documentation     => q[],
                        lazy_build        => 1
                      );


sub _build_compositions{ 
 my $self = shift;
 my @compositions = ();
  if ($self->has_rpt_list) {
    my $class = 'npg_tracking::glossary::composition::factory::rpt_list';
    @compositions = map { $_->create_composition() }
                    map { $class->new(rpt_list => $_) }
                    $self->rpt_list();
  } elsif ($self->has_composition_path) {
    @compositions = map { npg_tracking::glossary::composition->thaw($_) }
                    map { read_file $_ }
                    glob( join q[ ],
                    map { "$_/*.composition.json" }
                    $self->composition_path() );
  }

return $compositions[0];
}

=head2 repository

The repository root directory.

=cut

has q{repository} => (
  isa           => q{Str},
  is            => q{ro},
  required      => 0,
  predicate     => q{has_repository},
  default       => $ENV{NPG_REPOSITORY_ROOT},
  documentation => q{The repository root directory},
);

=head 2 load_to_qc_database_only

=cut 

has q{load_to_qc_database_only} => (
   isa           => q{Bool},
   is            => q{ro},
   documentation => q{Flag to run qc database loading},
);

=head2 can_run

=cut

sub can_run {
    my $self = shift;
     if (! $self->repository){ $self->log('NPG_REPOSITORY_ROOT or --repository not specified') ; return 0 };
    ###return 0 if previous submission for this - check in events db
     if ($self->exists_in_eventsdb()){ return 0 }

return 1;
}


has q{events_login} => (
                 isa        => q{HashRef},
                 is         => q{ro},
                 lazy_build => 1,);
sub _build_events_login {
    return from_json(read_file(glob "~/.npg/event_warehouse_ro.json"));    
}	

sub exists_in_eventsdb {
    my $self = shift;
    my $Hr = $self->events_login();
    my $sql = $self->events_by_rpt_list();
    my $dbh  = DBI->connect($Hr->{'dsn'},$Hr->{dbuser},$Hr->{dbpass},{RaiseError => 1,AutoCommit => 0,});
    #my $sth = $dbh->prepare($sql) or croak "Cannot prepare query :" . $dbh->errstr;
    #   $sth->execute or croak "Cannot execute query :" . $dbh->errstr;
    #my @row  = $sth->fetchrow_array;
    my @row;
    eval {
      @row  = $dbh->selectrow_array($sql);
    };
       $dbh->disconnect; 
    if (@row){
        $self->log("Found in Event warehouse event_id:$row[0] $row[1] file:$row[3] $row[6] $row[7]");
        return 1;
    }
    $self->log($self->rpt_list," not found in the Event warehouse");
    return 0; 
}

sub events_by_rpt_list {
    my $self = shift;
    #my $rpt = $self->rpt_list;##temp!  
    my $rpt =  $self->compositions->freeze2rpt;
    return qq{SELECT events.id event_id,rpt.value rpt, friendly_name,CONCAT(friendly_name,'/',SUBSTRING_INDEX(file.value, '/', -1)) file,occurred_at,filem.value md5,event_types.key event_type, events.updated_at FROM events JOIN metadata file ON file.event_id=events.id AND file.key='file_path' JOIN metadata filem ON filem.event_id=events.id AND filem.key='file_md5' JOIN metadata rpt ON rpt.event_id=events.id AND rpt.key='rpt_list' JOIN roles ON roles.event_id=events.id JOIN subjects ON subject_id=subjects.id JOIN event_types ON event_types.id=events.event_type_id JOIN mlwarehouse.sample ON sample.supplier_name=friendly_name WHERE rpt.value = "$rpt" GROUP BY friendly_name, file.value, filem.value  ORDER BY occurred_at;};
}

=head2 qc_schema
 
DBIx schema class for npg_qc access.

=cut

has q{qc_schema} => (
                isa        => q{npg_qc::Schema},
                is         => q{ro},
                required   => 1,
                lazy_build => 1,);
sub _build_qc_schema {
  require npg_qc::Schema;
  return npg_qc::Schema->connect();
}

=head2 run

=cut

sub run {
    my $self = shift;

    return 0 if ! $self->can_run();
    if ($self->make_commands()){ $self->run_wr() }
    return 1;
}


=head2 library_qc_values_set-for_all

=cut 

sub library_qc_values_set_for_all {
   my $self = shift;

   my $name = $self->_product->file_name_root();
   my $rpt = $self->_product->rpt_list();

#   my @seqqc = $self->_product->final_seqqc_objs($self->qc_schema);
#      @seqqc or $self->logcroak("Product $name, $rpt are not all Final seq QC values");

#   if (not all { $_->is_accepted }  @seqqc) {
#      $self->log("Product $name, $rpt are not all Final Accepted seq QC values");
#      return 0;
#    }       

    my $libqc_obj = $self->_product->final_libqc_obj($self->qc_schema);

    $libqc_obj or $self->logcroak("Product $name, $rpt is not Final lib QC value");

    if ($libqc_obj->is_accepted) {
       $self->log("Product $name, $rpt is for release (passed manual QC)");
       return 1;
    }
    $self->log("Product $name, $rpt library qc values are not all set");
return 0;
}

=head2 make_commands

=cut

sub make_commands {
    my $self = shift;

     $self->make_merge_dir($self->compositions->freeze2rpt);
  my $name = $self->_product->file_name_root();
  my $rpt = $self->_product->rpt_list();
     $self->composition_id($name);
    print $self->archive_path();
   

  print " Cache name ", $self->_cache_name(), " product ",$self->_product->file_path($self->_cache_name());
  
  my $qc_path  = $self->_product->qc_out_path($self->archive_path());
  print "qc_path $qc_path\n";

  if (! $self->library_qc_values_set_for_all()){ return 0 }

my $command_input_fh = IO::File->new($self->commands_file,'>') or croak q[cannot open ], $self->commands_file," : $OS_ERROR\n";

##### if QC result and sent, do nothing
##### if QC not yet loaded to QC database -> run npg_pipeline_post_qc_review with --function order upload_illumina_analysis_to_qc_database and --function order  upload_auto_qc_to_qc_database 

###e.g. select id_seq_composition,c_id_run,composition_digest from v_samtools_stats where composition_digest = 'f8d0812aa68f8378d2cdc6f6922cd4ceeba9b1af1cb655069fcbc51e7373a2e0';

####export CLASSPATH=/software/npg/20190626/jars;export NPG_CACHED_SAMPLESHEET_FILE=/lustre/scratch113/merge_component_results/5392/48/bc/48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95/48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95.csv;export NPG_REPOSITORY_ROOT=/lustre/scratch113/npg_repository;export PATH=/software/npg/20190809/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin;export PERL5LIB=/software/npg/20190809/lib/perl5; umask 002 && npg_pipeline_post_qc_review  --verbose  --function_order archive_to_s3 --function_order notify_product_delivery --label rt668088 --runfolder_path /lustre/scratch113/tickets/rt668088/logs/48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95 --archive_path /lustre/scratch113/merge_component_results/5392/48/bc/ --product_rpt_list '27312:1:15;27312:2:15;27312 :3:15;27312:4:15;28780:2:5' --analysis_path /lustre/scratch113/tickets/rt668088/logs/48bc99bfddb379ea9569037c9054d3a03a7a0aaa8fe82804e0f317991e501b95 --function_order update_ml_warehouse


my $npg_cached_samplesheet_file = q[NPG_CACHED_SAMPLESHEET_FILE=] . $self->archive_path . $self->composition_id . q[/] . $self->composition_id . q[.csv];

if ($self->load_to_qc_database_only){ ##
    my $qc_load_cmd = q[];
       $qc_load_cmd =qq[ export $npg_cached_samplesheet_file; umask 0002 && npg_pipeline_post_qc_review  --verbose  --function_order  upload_auto_qc_to_qc_database --function_order  upload_illumina_analysis_to_qc_database --label rt]. $self->rt_ticket . q[ --runfolder_path ] . $self->log_dir . q[ --archive_path ] . $self->archive_path . q[ --product_rpt_list ] . $self->rpt_list . q[ --analysis_path ] . $self->log_dir . qq[ --qc_path $qc_path ]; 

    print "**$qc_load_cmd\n";

$self->_command_to_json({
                         cmd      => $qc_load_cmd,
                         rep_grp  => q[rt].$self->rt_ticket,
                         disk     => 150,
                         },q[npg_qc],$command_input_fh);

    return 1;
  }


### Manually need to check that the above qc database jobs have finished first
my $archive_cmd = q[];
   $archive_cmd =qq[ export $npg_cached_samplesheet_file; umask 0002 && npg_pipeline_post_qc_review  --verbose  --function_order archive_to_s3 --function_order notify_product_delivery --label rt]. $self->rt_ticket . q[ --runfolder_path ] . $self->log_dir . q[ --archive_path ] . $self->archive_path . q[ --product_rpt_list ] . $self->rpt_list . q[ --analysis_path ] . $self->log_dir . q[ --function_order update_ml_warehouse ]; 

    print "**$archive_cmd\n";

$self->_command_to_json({
                         cmd      => $archive_cmd,
                         rep_grp  => q[rt].$self->rt_ticket,
                         disk     => 150,
                         },q[s3_mlwh],$command_input_fh);



return 1;
}

=head2 _command_to_json

=cut

sub _command_to_json {
    my $self     = shift;
    my $hr       = shift;
    my $analysis = shift;
    my $command_fh = shift;

       $hr->{priority} = $WR_PRIORITY;
    my $cmd = $hr->{cmd};
   
    my $composition_id = $self->composition_id;
    #my $out_file_path = $self->out_dir . q[/log/] . $self->composition_id;
     my $out_file_path = $self->log_dir;
    if ($analysis && ($analysis !~ /^_/smx)){ $out_file_path .= q[.] };    
        $out_file_path .= $analysis ? $analysis : q[];
        $out_file_path .= q[.out];


       $hr->{cmd} = qq[($cmd) 2>&1 | tee -a \"$out_file_path\"];

       if (! $hr->{memory}){ $hr->{memory} = $MEMORY_4000M };
    my $json = JSON->new->allow_nonref;
    my $json_text   = $json->encode($hr);

    print {$command_fh} $json_text,"\n"  or $self->log(qq[Can't write to commands file: $ERRNO]);;
return;
}


=head2 run_wr

=cut

sub run_wr {
    my $self = shift;

    my $wr_cmd = q[wr  add --cwd /tmp --retries 0  --override 2 --disk 0 --rep_grp top_up_merge_archive --env '];
       $wr_cmd .= $self->wr_env();
       $wr_cmd .= q[' -f ] . $self->commands_file;
       $wr_cmd .= q[ --deployment ] . $self->wr_deployment;


    $self->log("**Running $wr_cmd**");

   if (! $self->dry_run ){
     my $wr_fh = IO::File->new("$wr_cmd |") or die "cannot run cmd\n";
     while(<$wr_fh>){}
     $wr_fh->close();
}

    return 1;
}


__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Moose

=item MooseX::StrictConstructor

=item Moose::Meta::Class

=item namespace::autoclean

=item Readonly

=item IO::File 

=item English

=item File::Slurp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

