use Dancer2;
use DBI;
use File::Spec;
use File::Slurper qw/ read_text /;
use Template;
use Text::CSV;
use Data::Dumper;
use File::Slurp;
use strict;
use warnings;
# Expose a rest service using Dancer2
# Command Line argument: Port, Driver, Server, Dbname, uid, pwd 
# comments starting with ###DEF are displayed in the / service. 3 dashes separated the service form the description
# perl .\dancr.pl 3333 MSSQL bi.ange.dl,41433 mydb userid secret
set 'logger'       => 'console';
set 'log'          => 'debug';
set 'show_errors'  => 1;
set 'startup_info' => 1;
set 'warnings'     => 1;
set port         => $ARGV[0]; # port number
my $DbDriver = $ARGV[1];
my $DbServer = $ARGV[2];
my $DbName = $ARGV[3];
my $UId = $ARGV[4]; #user id
my $Pwd = $ARGV[5];
#------------------------
# Connection to the DB, uses the command line argument and open with SQLite or MSSQL with SQL authentication
#------------------------ 
sub connect_db {
	my $dbh;
	if ($DbDriver eq "SQLite") {$dbh = DBI->connect("dbi:SQLite:dbname=".setting('$dbName')) or die $DBI::errstr;}
	elsif ($DbDriver eq "MSSQL")  {
		$dbh = DBI->connect("DBI:ODBC:Driver={SQL Server};Server=$DbServer;Database=$DbName;UID=$UId;PWD=$Pwd") or die("\nCONNECT ERROR:\n$DBI::errstr");}
    return $dbh;
}
#------------------------
# Read a table given an SQL statement and a DB connection. Return a CSV with the result.
#------------------------
sub ReadTable {
	my $db	= shift;
	my $sql = shift;
    my $sth = $db->prepare($sql) or die $db->errstr;
    $sth->execute or die $sth->errstr;	
	my $data = $sth->fetchall_arrayref; # retrieve all lines
	my $res;
	my $fields = $sth->{NAME}; # returns the column names in the same order as in the array.
	my $csv = Text::CSV->new( { binary => 1, eol => "\r\n" } );
	$csv->combine(@$fields); #compose the header CSV
	$res .= $csv->string();
	foreach my $x (@$data) {
		$csv->combine(@$x);
		$res .= $csv->string(); #add a CSV line.
		}
	return $res;
	} 
#------------------------
# Service /services gives a list of all the available methods with some HTML formatting for ease of read.
# Each service name is extracted from the source script with a special prefix ###DEF 
# Idea is to have the prefix placed in the source file near the service definition.
#------------------------
###DEF get /services---get this page
get '/services' => sub {
	my @sourceCode = split /\n/ , read_file( $0 ) ; # get current script code
	my $res;
	$res = '<H1>Rest services for current server</H1>';
	$res .= '<table><tr><td><b><u>Service</td><td><b><u>Description</td></tr>';
	for my $sourceLine (@sourceCode) {
		next if '###DEF' ne substr($sourceLine,0,6); # Only retains lines that started with ###DEF
		$sourceLine = substr($sourceLine,7);
		(my $serv, my $desc) = split /---/,$sourceLine,2; # 3 dashes separate the service name from the description
		$res .= "<tr><td><b>$serv</b></th><td>$desc</td></tr>";
		}	
	$res .= '</table>';
	return $res;
};
###DEF get /showtables---get a list of all the tables.
get '/showtables' => sub {
    my $db = connect_db();
    my $sql = 'select distinct TABLE_SCHEMA,TABLE_NAME from information_schema.columns'; #SQL92 standard for table list.
	
	return ReadTable($db,$sql);
};
###DEF get /readtable?table=tablename---read the tablename and return all rows and columns parameters
get '/readtable' => sub {
	my $db = connect_db();
	my $table = param 'table' ;
	return ReadTable($db,"select * from $table");
}; 
###DEF get /dosql?sql=sqlstring---execute adhoc sql. use %20 for spaces
get '/dosql' => sub {
	my $db = connect_db();
	my $sql = param 'sql' ;
	return ReadTable($db,$sql);
}; 
start; #dancer2 start