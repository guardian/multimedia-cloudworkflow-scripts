<?php
#This script looks up a video in the interactivepublisher database and returns a redirect if it can be found

$dbh=mysql_connect('gnm-mm-interactivevids.cuey4k0bnsmn.eu-west-1.rds.amazonaws.com','iviewer','ByAhtY3hVJQ9tMjU');
mysql_select_db('interactivevids');

if($_GET['file'] or $_GET['filebase']){
	$fn=$_GET['file'];
	if(preg_match("/[;']/",$fn)){
		header('HTTP/1.0 400 Bad Request',true,400);
		exit;
	}
	$fn=mysql_real_escape_string($fn);
	$q="select * from idmapping where filebase='$fn'";
	$result=mysql_query($q);
	$idmappingdata=mysql_fetch_assoc($result);

	$contentid=$idmappingdata['contentid'];
} elseif($_GET['octopusid']){
	$octid=$_GET['octopusid'];
	if(! preg_match("/^\d+$/",$octid)){
		header('HTTP/1.0 400 Bad Request',true,400);
		exit;
	}
	$q="select * from idmapping where octopus_id=$octid";
	$result=mysql_query($q);
	$idmappingdata=mysql_fetch_assoc($result);
	$contentid=$idmappingdata['contentid'];
} else {
	header('HTTP/1.0 404 Not Found',true,404);
}

#Step 1.
#The FCS id uniquely identifies the version (as opposed to octopus_id uniquely identifies the title which can have multiple versions.
#Versions can have subtly different bitrates AND arrive at different times, so just searching versions with a sort order can return old results no matter what.
#So, the first step is to find the most recent FCS ID and then search with that
#Some entries may not have FCS IDs, and if uncaught this leads to all such entries being treated as the same title.
#So, we iterate across them all and get the first non-empty one. If no ids are found then we must fall back to the old behaviour (step 3)
$q="select fcs_id from encodings where contentid=$contentid order by lastupdate desc";
$fcsresult=mysql_query($q);
if(!$fcsresult){
	header("HTTP/1.0 500 Database query error");
	exit;
}
while($fcsdata=mysql_fetch_assoc($fcsresult)){
	if($fcsdata['fcs_id'] and strlen($fcsdata['fcs_id'])>1){
		$fcsid=$fcsdata['fcs_id'];
#		print "got fcsid $fcsid";
		break;
	}
}
#die("testing, content id=$contentid fcs id =$fcsid");

#Step 2.
#Look for videos ordered by descending bitrate that belong to the given ID (if we got one). If not then fall through.
#If none are found, AND we have allow_old set, then re-do the search over everything (and potentially return an old result)
if($fcsid and $fcsid!=''){
	$q="select * from encodings left join mime_equivalents on (real_name=encodings.format) where fcs_id='$fcsid' order by vbitrate desc";
#	print "searching by fcsid $fcsid...\n";

	$contentresult=mysql_query($q);
	if(!$contentresult){
		header("HTTP/1.0 500 Database query error");
		exit;
		#print "unable to run query $q";
	}
#	die("testing");
}

#Step 3.
#fall back to the old behaviour if nothing was found. this usually means an update is in progress.
#allow_old will enable this behaviour in the next version
if($fcsid=='' or mysql_num_rows($contentresult)==0){
#	if(! $_GET['allow_old']){
#		header("HTTP/1.0 404 No content found");
#		exit;
#	}
#	print "old search fallback...\n";
	$q="select * from encodings left join mime_equivalents on (real_name=encodings.format) where contentid=$contentid";
	if(! $_GET['allow_old']){
	       $q=$q." and lastupdate>='".$idmappingdata['lastupdate']."'";
	}
	#$q=$q." order by lastupdate desc";
	$q=$q." order by vbitrate desc,lastupdate desc";
	
        $contentresult=mysql_query($q);
        if(!$contentresult){
                header("HTTP/1.0 500 Database query error");
                exit;
                #print "unable to run query $q";
        }
}


$have_match=0;

#print "Requested format: '".$_GET['format']."'\n";

while($data=mysql_fetch_assoc($contentresult)){
#	var_dump($data);
#	print $_GET['format'] ." ? " .$data['format']."\n";

	if(array_key_exists('format',$_GET))
		if($data['format']!=$_GET['format'] && $data['mime_equivalent']!=$_GET['format']) continue;
	if(array_key_exists('need_mobile',$_GET)){
		#print "checking mobile...\n";	
		if($data['mobile']!=1) continue;
	}
	if(array_key_exists('minbitrate',$_GET))	
		if($data['vbitrate']<$_GET['minbitrate']) continue;
	if(array_key_exists('maxbitrate',$_GET))
		if($data['vbitrate']>$_GET['maxbitrate']) continue;
       if(array_key_exists('minheight',$_GET))
	         if($data['frame_height']<$_GET['minheight']) continue;
	if(array_key_exists('maxheight',$_GET))
                if($data['frame_height']>$_GET['maxheight']) continue;
	if(array_key_exists('minwidth',$_GET))
                if($data['frame_width']<$_GET['minwidth']) continue;
	if(array_key_exists('maxwidth',$_GET))
                if($data['frame_width']>$_GET['maxwidth']) continue;
	$data['url']=preg_replace("/\|/","",$data['url']);

	if($_GET['poster']){	#set the poster=arg to anything to get poster image instead
		preg_match("/^(.*)\.[^\.]+$/",$data['url'],$matches);
		$posterurl=$matches[1]."_poster.jpg";
		header("Location: ".$posterurl);
	} else {
		header("Location: ".$data['url']);
	}
	exit;
}
print "No content found.\n";
header("HTTP/1.0 404 Not Found");
?>
