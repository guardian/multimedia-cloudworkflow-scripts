#!/usr/bin/perl

use Getopt::Long;
use DBI;
use Amazon::SNS;
use XML::SAX;
use CDS::Parser::saxmeta;
use CSLogger;

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

#START MAIN
my $inmeta;
our $debuglevel,$logid;

our $logger=CSLogger->new;
$logger->connect(database=>'interactivepublisherlog',
        host=>'***REMOVED***',
        username=>'iplog',
        password=>'fEG3wGnKszGEzE9S');

GetOptions("input-inmeta=s"=>\$inmeta,"debuglevel=i"=>\$debuglevel,"logid=s"=>\$logid);

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
push @files,$inmeta;

$logger->newjob(id=>$logid,files=>\@files,%{$metadata->{'meta'}});

if($loglevel>2){
	mylog($logid,"Dump of received metadata:\n");
	local $Data::Dumper::Pad="\t";
	mylog($logid,Dumper($metadata));
}

