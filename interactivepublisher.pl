#!/usr/bin/perl

my $version='$Rev$ $LastChangedDate$';

use Getopt::Long;
use DBI;
use Amazon::SNS;
use XML::SAX;
use CDS::Parser::saxmeta;
use CSLogger;
use Data::Dumper;
use LWP::UserAgent;
use File::Basename;

my $title_id_field="octopus ID";

our @filename_fields = qw/filename originalFilename/;

our $fieldmultipliers = [
    {
 	abitrate=>1,
	vbitrate=>1
    },
    {
	abitrate=>0.001,
	vbitrate=>0.001
    }
];

our $fieldmappings = [
    {
        abitrate=>"tracks:audi:bitrate",
        acodec=>"tracks:audi:format",
        format=>"movie:format",
        mobile=>"meta:for_mobile",
        multirate=>"meta:is_multirate",
        url=>"meta:cdn_url",
        vbitrate=>"tracks:vide:bitrate",
        vcodec=>"tracks:vide:format",
        aspect=>"meta:aspect_ratio",
        duration=>"movie:duration",
        fcs_id=>"meta:FCS asset ID",
        file_size=>"movie:size",
        frame_height=>"tracks:vide:height",
        frame_width=>"tracks:vide:width",
        octopus_id=>"meta:octopus ID"
#	filename=>"meta:filename"
    },
    {
        abitrate=>"tracks:audi:aac_settings_bitrate",
        acodec=>"tracks:audi:codec",
	format=>"movie:extension",
        mobile=>"meta:for_mobile",
        multirate=>"meta:is_multirate",
        url=>"meta:cdn_url",
        vbitrate=>"tracks:vide:h264_settings_bitrate",
        vcodec=>"tracks:vide:codec",
        aspect=>"meta:aspect_ratio", #can set this with derive_aspect_ratio in the feeding route
        duration=>"meta:durationSeconds",
        fcs_id=>"meta:itemId",
        file_size=>"movie:size", #should be set by check_fix_size in the feeding route
        frame_height=>"tracks:vide:height",
        frame_width=>"tracks:vide:width",
        octopus_id=>"meta:gnm_master_generic_titleid"
#	filename=>"meta:originalFilename"
    }
];

sub error
{
my ($id,$msg)=@_;

eval {
$logger->logerror(id=>$id,message=>$msg);
};

print STDERR $msg;
}

sub mylog
{
my ($id,$msg)=@_;

eval {
$logger->logmsg(id=>$id,message=>$msg);
};
if($@){
        print STDERR "Error trying to log to external logger: $@\n";
}

print STDERR $msg;
}

sub mywarn
{
my ($id,$msg)=@_;

eval {
$logger->logwarning(id=>$id,message=>$msg);
};
if($@){
        print STDERR "Error trying to log to external logger: $@\n";
}

print STDERR $msg;
}

sub get_value {
my($metadata,$path)=@_;

print "debug:get_value: given path $path\n" if($debuglevel>3);
my @pathsections=split /:/,$path;
print "debug:get_value: path sections are:\n" if($debuglevel>3);
local $Data::Dumper::Pad="\t";
print Dumper(\@pathsections) if($debuglevel>3);

my $ptr=$metadata;
foreach(@pathsections){
	print "\tdebug:get_value: got ".$ptr->{$_}." at $_\n" if($debuglevel>3);
	return undef unless($ptr->{$_});
	$ptr=$ptr->{$_};
}
print "debug:get_value: found it!\n" if($debuglevel>3);
return $ptr;
}


sub map_metadata {
my($metadata)=@_;
my %mapped_md;

for($n=0;$n<scalar @$fieldmappings;++$n){
    my $fieldmapping=$fieldmappings->[$n];
    print Dumper($fieldmapping);
    foreach(keys %{$fieldmapping}){
        unless($mapped_md{$_}){
            print "debug:map_metadata: mapping $_ from path ".$fieldmapping->{$_}." using mapping set $n...\n" if($debuglevel>3);

            my $mdvalue=get_value($metadata,$fieldmapping->{$_});
            if($mdvalue and $mdvalue !~ /^[^\w\d]*$/){
		if($fieldmultipliers->[$n] and $fieldmultipliers->[$n]->{$_}){
			$mdvalue=$mdvalue*$fieldmultipliers->[$n]->{$_};
		}
                $mapped_md{$_}=$mdvalue;
            } else {
                mywarn($logid,"WARNING: Metadata did not specify any value for field $_, mapping from ".$fieldmapping->{$_}."\n");
            }
        }
    }
}
return \%mapped_md;

}

sub log_success
{
my ($id,$msg)=@_;

eval {
$logger->logmsg(id=>$id,message=>$msg);
};
if($@){
        print STDERR "Error trying to log to external logger: $@\n";
}

print STDERR $msg;
}

sub read_inmeta {
my($filename)=@_;

if(not -f $filename){
	die "Inmeta file $filename does not exist.\n";
}

my $handler=CDS::Parser::saxmeta->new;
$handler->{'config'}->{'keep-spaces'}=1;
$handler->{'config'}->{'keep-simple'}=1;

eval {
	my $parser=XML::SAX::ParserFactory->parser(Handler=>$handler);
	$parser->parse_uri($filename);
};
if($@){
	die "Unable to read inmeta file:\n$@\n";
}

my $metadata=$handler->{'content'};
return $metadata;
}

sub get_contentid
{
my $metadata=shift;

my $filebase;
foreach(@filename_fields){
    $filebase=$metadata->{'meta'}->{$_};
    last if($filebase);
}
unless($filebase){
    error($logid,"Unable to find file name metadata from any key of @filename_fields");
    return -1;
}

#Chop any file extension of the provided filename to form our base
$filebase=~s/\.[^\.]+$//;

#Does this id already exist in the database?
my $sth=$dbh->prepare("SELECT * from idmapping WHERE filebase=?");
$sth->execute($filebase);
if($sth->rows>0){
	#If it does, return the ID that exists already
	my $data=$sth->fetchrow_hashref;
	if($debuglevel>1){	
		mylog($logid,"get_contentid: found following record for $filebase:\n");
		local $Data::Dumper::Pad="\t";
		mylog($logid,Dumper($data));
	}
	return $data->{'contentid'};
} else {
	#If it doesn't, try to return a new ID
	mylog($logid,"get_contentid: no record presently exists for $filebase.\n");
	my $sth;
	if($metadata->{'meta'}->{'octopus ID'}){
		$sth=$dbh->prepare("insert into idmapping (filebase,octopus_id,project) values (?,?,?)");
	#FIXME: these mappings should not be hard-coded like this!
		$sth->execute($filebase,$metadata->{'meta'}->{'octopus ID'},$metadata->{'meta'}->{'project_name'});
	} else {
                $sth=$dbh->prepare("insert into idmapping (filebase,project) values (?,?)");
        #FIXME: these mappings should not be hard-coded like this!              
  $sth->execute($filebase,$metadata->{'meta'}->{'project_name'});
	}

	$sth=$dbh->prepare("SELECT * from idmapping WHERE filebase=?");
	$sth->execute($filebase);
	if($sth->rows<1){
		error($logid,"Unable to create a new record for entry '$filebase'\n");
		return undef;
	}
	my $data=$sth->fetchrow_hashref;
	return $data->{'contentid'};
}
return undef;
}

#This function uses the "mapped data" structure and converts it into an SQL insert statement which
#is then executed.
sub add_encoding {
my $mapped_data=shift;

my $fieldnames;
my $assignment_values;
my @args;
foreach(keys %{$mapped_data}){
	$fieldnames=$fieldnames.$_.",";
	$assignment_values=$assignment_values."?,";
	push @args,$mapped_data->{$_};
}
chop $fieldnames;
chop $assignment_values;

my $st="insert into encodings ($fieldnames) values ($assignment_values)";
mylog($logid,"debug: add_encoding: I will execute $st\n") if($debuglevel>3);

eval {
	my $sth=$dbh->prepare($st);
	$sth->execute(@args);
};
if($@){
	error($logid,"ERROR: Unable to add new encoding record: $@\n");
	return 0;
}
return 1;
}

sub get_mimetype {
my $url=shift;

my $ua=LWP::UserAgent->new;

my $response=$ua->head($url);
if($response->is_success){
	if($response->header('Content-Type') eq "text/plain"){
		return "application/x-mpegURL";	#assume that anything reporting itself as text is actually an m3u8
	}
	return $response->header('Content-Type');
} else {
	error($logid,"ERROR: Unable to get mimetype for the url '$url': ".$response->status_line.".\n");
	return undef;
}
}

#START MAIN
my $inmeta,$keepfile;
our $debuglevel,$logid;

our $logger=CSLogger->new;
$logger->connect(database=>'interactivepublisherlog',
        host=>'***REMOVED***',
        username=>'iplog',
        password=>'fEG3wGnKszGEzE9S');

GetOptions("input-inmeta=s"=>\$inmeta,"debuglevel=i"=>\$debuglevel,"logid=s"=>\$logid,"keep"=>\$keep);

eval {
        $logger->logmsg($logid,"Interactive publisher script starting up...\n");
};

my $metadata;
eval {
	$metadata=read_inmeta($inmeta);
};
if($@){
	error($logid,"Unable to read incoming metadata: $@\n");
	exit 1;
}

my @files;
push @files,$metadata->{'filename'} if($metadata->{'filename'});
push @files,basename($metadata->{'cdn_url'}) if(scalar @files<1 and $metadata->{'cdn_url'});
push @files,$inmeta;

my $tempmeta=$metadata->{'meta'};
$logger->newjob(id=>$logid,files=>\@files,%{$tempmeta});

if($debuglevel>2){
	mylog($logid,"Dump of received metadata:\n");
	local $Data::Dumper::Pad="\t";
	mylog($logid,Dumper($metadata));
}

my $dsn="DBI:mysql:database=interactivevids;host=gnm-mm-interactivevids.cuey4k0bnsmn.eu-west-1.rds.amazonaws.com";

our $dbh=DBI->connect($dsn,"ivids","w4RAZKnZX2AYKZmp");

unless($dbh){
	error($logid,"Unable to connect to the database.  Please check that the server exists and all configuration is correct.\n");
	exit 2;
}

my $contentid=get_contentid($metadata);
if($contentid<0){
    error($logid,"Unable to get content ID");
    exit(1);
}

if($debuglevel>2){
	mylog($logid,"INFO: Got an ID of $contentid for the new database entry.\n");
}

my $mapped_data=map_metadata($metadata);
print Dumper($mapped_data);

if($mapped_data->{'url'}){
	$mapped_data->{'url'}=~s/\|$//;	#sometimes CDS leaves a trailing delimiter that must be removed
	$mapped_data->{'url'}=~s/,$//;	#to my knowledge nothing downstream does this, but might as well check it anyway...
	$mapped_data->{'format'}=get_mimetype($mapped_data->{'url'});
	if(not defined $mapped_data->{'format'}){
		print "-ERROR: Unable to retrieve information for ".$mapped_data->{'url'}."\n";
		goto done;
	}
}

if(not $mapped_data->{'vbitrate'}=~/^\d+$/ or $mapped_data->{'vbitrate'}<1){
	$mapped_data->{'vbitrate'}=$metadata->{'movie'}->{'bitrate'};
}

$mapped_data->{'contentid'}=$contentid;
my $r=add_encoding($mapped_data);

my $filename;
foreach(@filename_fields){
    $filename=$metadata->{'meta'}->{$_};
    last if($filename);
}

done:
if($r){
	log_success($logid,"SUCCESS: Completed run to add an encoding of type '".$mapped_data->{'format'}."' of file '".$filename."' under id ".$contentid." to the database.\n");
} else {
	error($logid,"ERROR: Unable to add ".$metadata->{'meta'}->{'filename'}." as a new record under id ".$contentid." to the database.\n");
}

unlink($inmeta) unless($keep);

