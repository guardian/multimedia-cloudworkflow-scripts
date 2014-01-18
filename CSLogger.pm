package CSLogger;

use strict;
use warnings;

use DBI;

sub new
{
my $class=shift;

my $self={};
bless($self,$class);
return $self;
}

sub connect
{
my ($self,%args)=@_;

my $dsn="DBI:mysql:database=".$args{'database'}.";host=".$args{'host'};
if($args{'port'}){
	$dsn=$dsn.";port=".$args{'port'};
}

$self->{'dbh'}=DBI->connect($dsn,$args{'username'},$args{'password'});

$self->get_internal_statusid;
}

#note - this depends on the bin_from_uuid and uuid_from_bin functions being manually defined in the database.

sub get_internal_jobid
{
my($self,$externalid)=@_;

die "Database not defined in get_internal_jobid" unless($self->{'dbh'});
my $sth=$self->{'dbh'}->prepare("select internalid from jobs where externalid=bin_from_uuid('$externalid')");
$sth->execute;

die "Unable to find record for external ID '$externalid'" unless($sth->rows>0);
return $sth->fetchrow_arrayref()->[0];
}

use Data::Dumper;

#note - this depends on the bin_from_uuid and uuid_from_bin functions being manually defined in the database.
sub newjob
{
my($self,%args)=@_;

my %meta;
#we recognise certain arguments. Anything else is generic metadata.
foreach(keys %args){
	next if($_ eq 'id');
	next if($_ eq 'files');
	$meta{$_}=$args{$_};
}

my $jobid=$args{'id'};

my $dbh=$self->{'dbh'};
$dbh->do("insert into jobs (externalid) values (bin_from_uuid('".$args{'id'}."'))");

#this will throw an exception if it fails, the caller should catch it.
my $internalid=$self->get_internal_jobid($args{'id'});

print Dumper(\%args);
foreach(@{$args{'files'}}){
	$dbh->do("insert into jobfiles (jobid,filename) values ($internalid,'$_')");
}

foreach(keys %meta){
	#print "insert into jobmeta (jobid,identifier,value) values ($internalid,'$_','".$meta{$_}."')";
	my $sth=$dbh->prepare("insert into jobmeta (jobid,identifier,value) values (?,?,?)");
	$sth->execute($internalid,$_,$meta{$_});
}

return $internalid;
}

sub get_internal_statusid
{
my($self,$name)=@_;

my $sth=$self->{'dbh'}->prepare("select * from status order by statusid");
$sth->execute;

my %statuses;
while(my $data=$sth->fetchrow_hashref){
	$self->{'statuses'}->{$data->{'desc'}}=$data;
}

return $statuses{$name} if($name);
return 1;
}

sub internal_log
{
my($self,%args)=@_;

foreach(qw/priority id message/){
	die "You must define $_ when calling logmsg" unless($args{$_});
}

$args{'message'}=~s/\'/\\\'/g;

my $sth=$self->{'dbh'}->prepare("insert into log(externalid,log,status) values(bin_from_uuid('".$args{'id'}."'),?,'".$args{'priority'}->{'statusid'}."')");
$sth->execute($args{'message'});
}

sub logfatal
{
my($self,%args)=@_;

$self->internal_log(priority=>$self->{'statuses'}->{'fatal'},%args);
}

sub logerror
{
my($self,%args)=@_;

$self->internal_log(priority=>$self->{'statuses'}->{'error'},%args);
}

sub logwarning
{
my($self,%args)=@_;

$self->internal_log(priority=>$self->{'statuses'}->{'warning'},%args);
}

sub logmsg
{
my($self,%args)=@_;

$self->internal_log(priority=>$self->{'statuses'}->{'log'},%args);
}

sub logdebug
{
my($self,%args)=@_;

$self->internal_log(priority=>$self->{'statuses'}->{'debug'},%args);
}

1;

