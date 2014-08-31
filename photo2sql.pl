#!/usr/bin/perl
use strict;
use File::Find;
use Cwd;
use Data::Dumper;
use Image::ExifTool qw(:Public);
use Getopt::Long;
use DBI;

my $DATABASE = 'shots.sqlite';
my %tags = (
            'FocalLength'      => 'focal_len:integer',
            'ScaleFactor35efl' => 'crop_factor:real',
            'Aperture'         => 'aperture:real',
            'Make'             => 'make:text',
            'Model'            => 'camera:text',
            'Lens'             => 'lens:text',
            'ShutterSpeed'     => 'shutter_speed:real',
            'ISO'              => 'iso:integer',
            'DateTimeOriginal' => 'time_stamp:date'
           );
my ($init_database, $path_to_photos, $dumpsql) = ();
our ($dbh, $recno) = ();

sub usage
{
    printf "Usage:\n%s [--init] --path <dir>\n", $0;
    printf "%s --dumpsql --path <dir>\n",        $0;
    die;
}

sub init
{
    unless (
            GetOptions(
                       "init"    => \$init_database,
                       "dumpsql" => \$dumpsql,
                       "path=s"  => \$path_to_photos
                      )
           )
    {
        usage();
    }

    usage() if ($path_to_photos eq '');

    if (!$dumpsql)
    {
        $dbh = DBI->connect("dbi:SQLite:dbname=shots.sqlite", "", "")
          or die("Unable to create database shots.sqlite");
    }
    create_DB_table($dbh) if ($init_database or $dumpsql);

    if ($path_to_photos !~ /^\//)
    {    # not absolute path
        $path_to_photos = getcwd() . '/' . $path_to_photos;
    }

    printf "-- Processing files in %s\n", $path_to_photos;
}

sub create_DB_table($)
{
    my $dbh = shift;

    my $create_tbl_sql = 'create table shots(';
    my @columns        = values(%tags);
    map { s/:/ /; } @columns;
    $create_tbl_sql .= join(',', @columns)
      . ", primary key (time_stamp,aperture,shutter_speed));\nbegin transaction;";

    if ($dumpsql)
    {
        printf "%s\n\n", $create_tbl_sql;
    }
    else
    {
        $dbh->do($create_tbl_sql) or die "Unable to create table shots\n";
    }
}

sub process_file
{
    my $fname = $File::Find::name;

    unless ($fname =~ /(nef|cr.|pef|orw|dng|jpg|jpeg|arw|mov|sr.)$/i)
    {
        # known file types only
        return;
    }

    unless (-f $fname)
    {
        return;    # we need regular files only
    }

    if ((stat($fname))[7] < 200 * 1024)
    {
        # skip anything smaller than 200k
        return;
    }

    my $exifTool = new Image::ExifTool;
    $exifTool->ExtractInfo($fname);

    my @available_tags;

    my $result;

    $result = $exifTool->GetInfo(\@available_tags);

    unless (grep(/Aperture/, @available_tags))
    {
        #print "No exif info found, skipping\n";
        return;
    }

    my %shot = ();
    my $tag;

    # populate %shot with required tags only. Tags list defined in %tags
    foreach $tag (keys %tags)
    {
        if (grep(/^$tag$/, @available_tags))
        {
            $shot{$tag} = $exifTool->GetValue($tag);
        }
        else
        {
            $shot{$tag} = 'NULL';
        }
    }

    # mangle some EXIF tags values

    # convert shutter speed into numeric value if it's like 1/320
    if ($shot{ShutterSpeed} =~ /(\d+)\/(\d+)/)
    {
        $shot{ShutterSpeed} = $1 / $2 if ($2 > 0);
    }

    # remove units (mm for all my cameras) from FocalLength
    $shot{FocalLength} =~ s/^([0-9.]+).*/$1/;

    # construct SQL insert from %shot
    my ($tag_names_DBstr, $tag_values_DBstr, $tag_type, $tag_value) = ();
    foreach $tag (keys %shot)
    {
        $tag_names_DBstr =
          $tag_names_DBstr . (split(/:/, $tags{$tag}))[0] . ',';
        $tag_type = (split(/:/, $tags{$tag}))[1];
        $tag_value = $shot{$tag};
        if ($tag_type =~ /(text|date)/i and $tag_value ne 'NULL')
        {
            $tag_value = "'" . $tag_value . "'";
        }
        $tag_values_DBstr = $tag_values_DBstr . $tag_value . ',';
    }
    chop $tag_names_DBstr  if ($tag_names_DBstr  =~ /,$/);
    chop $tag_values_DBstr if ($tag_values_DBstr =~ /,$/);

    my $insert = sprintf "insert into shots (%s) values (%s);",
      $tag_names_DBstr, $tag_values_DBstr;

    if ($dumpsql)
    {
        printf "%s\n", $insert;
    }
    else
    {
        $dbh->do($insert);    # might fail here due to primary key constraint
        printf "%s done\n", $fname;
    }
}

################## main program

init();

find(\&process_file, ($path_to_photos));

$dbh->do('commit');
$dbh->disconnect unless ($dumpsql);

