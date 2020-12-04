#!/usr/bin/perl
#

use strict;
use Data::Dumper;
use Digest::MD5 qw(md5_hex);
use Fcntl;
use File::Basename;
use Getopt::Long;
use Parallel::ForkManager;
use POSIX;

use constant K => 2**10;
use constant M => 2**20;
use constant G => 2**30;

use constant STAT_DEV     => 0;
use constant STAT_INODE   => 1;
use constant STAT_MODE    => 2;
use constant STAT_NLINK   => 3;
use constant STAT_UID     => 4;
use constant STAT_GID     => 5;
use constant STAT_RDEV    => 6;
use constant STAT_SIZE    => 7;
use constant STAT_ATIME   => 8;
use constant STAT_MTIME   => 9;
use constant STAT_CTIME   => 10;
use constant STAT_BLKSIZE => 11;
use constant STAT_BLOCKS  => 12;

use constant PW_NAME    => 0;
use constant PW_PASS    => 1;
use constant PW_UID     => 2;
use constant PW_GID     => 3;
use constant PW_QUOTA   => 4;
use constant PW_COMMENT => 5;
use constant PW_GECOS   => 6;
use constant PW_DIR     => 7;
use constant PW_SHELL   => 8;
use constant PW_EXPIRE  => 9;

use constant GR_NAME    => 0;
use constant GR_PASS    => 1;
use constant GR_GID     => 2;
use constant GR_MEMBERS => 3;

my %OPTS = (
  'concurrent' => 10,
  'debug' => 0,
  'start-chunk' => 0,
  'start-byte' => 0,
  'chunk-size' => 50 * M,
  'block-size' => 1 * M,
);

GetOptions(
  \%OPTS,
  'concurrent=i',
  'debug+',
  'start-chunk=i',
  'start-byte=i',
  'chunk-size=i',
  'block-size=i',
  'preserve|perms|p',
  'parent|path',
);

my ($src, $dst) = @ARGV;

my $ext = '';

my $MAX_JOBS = $OPTS{'concurrent'};

my $src_size = -s "$src" || die;
my $chunks = floor($src_size / $OPTS{'chunk-size'});

print "size: $src_size\n" if $OPTS{'debug'};
print "chunk_size: $OPTS{'chunk-size'}\n" if $OPTS{'debug'};
print "chunks: $chunks\n" if $OPTS{'debug'};

$OPTS{'start-chunk'} = floor($OPTS{'start-size'} / $OPTS{'chunk-size'}) if ($OPTS{'start-size'});

my $pm = Parallel::ForkManager->new($MAX_JOBS);

my $dir = dirname("$dst");
my $base = basename("$dst");

print "src: $src\n" if( $OPTS{'debug'} );
print "dst: $dst\n" if( $OPTS{'debug'} );

### execute(
###    'mkdir',
###    '-p',
###    $dir
### ) if $OPTS{'parent'};
### 
### foreach my $chunk ($OPTS{'start-chunk'}..$chunks) {
###   my $pid = $pm->start and next;
### 
###   copyChunk( $src, "$dst$ext", $chunk, $src_size );
### 
###   $pm->finish;
### }
### 
### $pm->wait_all_children;

copyPerms( $src, "$dst$ext" ) if $OPTS{'preserve'};

exit 0;

sub modText {
  my( $mode, $set ) = @_;

  my $flags;
  my $sid = 01;
  my $sid_bits = ($mode & 07000) >> 9;

  for(my $i=0; $i<3; $i++) {
    my $oct = $mode & 07;

    my $r = ( $oct & 04 ) ? 'r' : '-';
    my $w = ( $oct & 02 ) ? 'w' : '-';
    my $x = ( $oct & 01 ) ? 'x' : '-';

    if ( $sid_bits & $sid ) {
      my $s = ( $sid > 1 ) ? 's' : 't';
      $x = ( $x eq '-' ) ? uc($s) : lc($s)
    }
    $flags = "$r$w$x$flags";

    $sid <<= 1;
    $mode >>= 3;
  }

  return $flags;
}

sub copyPerms {
  my ( $src, $dst ) = @_;

  my @stat = stat($src);

  copyOwner( $src, $dst, @stat );
  copyMode( $src, $dst, @stat );

  copyContext( $src, $dst );
  copyXattr( $src, $dst );

  # Set the file times
  utime $stat[STAT_ATIME],
        $stat[STAT_MTIME],
        $dst or die "$!: $dst";
}

sub copyOwner {
  my ( $src, $dst, @stat ) = @_;

  @stat = stat($src) if $#stat < 0;

  # Set the file owner
  # MUST BE FIRST, chown will clear setuid bits
  my @pwuid = getpwuid($stat[STAT_UID]);
  my @grgid = getgrgid($stat[STAT_GID]);
  printf "Setting owner/group to '%s:%s' on %s\n",
    $pwuid[PW_NAME],
    $grgid[GR_NAME],
    $dst
    if $OPTS{'debug'};

  chown $stat[STAT_UID],
        $stat[STAT_GID],
        $dst or die "$!: $dst";
}

sub copyMode {
  my ( $src, $dst, @stat ) = @_;

  @stat = stat($src) if $#stat < 0;

  # Set the file mode
  # MUST chown FIRST, chown will clear setuid bits
  printf "Setting mode to 0%04o '%s' on %s\n",
    $stat[STAT_MODE] & 07777,
    modText( $stat[STAT_MODE] & 07777 ),
    $dst if $OPTS{'debug'};

  chmod $stat[STAT_MODE] & 07777,
        $dst or die "$!: $dst";
}

sub copyXattr {
  my ( $src, $dst ) = @_;

  # Unknown how to do this, or if possible
}

sub copyContext {
  my ( $src, $dst ) = @_;

  # fugly but quick solution
  open(GETCON, "ls --scontext \"$src\"|") or die $!;
  my $context = <GETCON>;
  close( GETCON );

  chomp $context;
  $context =~ s/\s.*$//;

  print "Setting context '$context' on $dst\n" if $OPTS{'debug'};

  my @args = ('chcon', $context, $dst);
  system(@args) == 0 or die "system @args failed: $?"
}

sub copyChunk {
  my ( $src, $dst, $chunk, $size ) = @_;

  sysopen( IN, "$src", O_RDONLY );
  sysopen( OUT, "$dst", O_RDWR | O_CREAT );
  binmode(IN);
  binmode(OUT);

  my $start = $chunk * $OPTS{'chunk-size'};
  my $remaining = $size - $start;
  $remaining = $OPTS{'chunk-size'} if $remaining > $OPTS{'chunk-size'};

  if (isCopied( \*IN, \*OUT, $start, $remaining )) {
    printf "SKIPING chunk %d; %d - %d (%d)\n", $chunk, $start, $start + $remaining - 1, $remaining if $OPTS{'debug'} > 1;
    return;
  }

  sysseek(IN, $start, SEEK_SET);
  sysseek(OUT, $start, SEEK_SET);

  printf "Copying chunk %d; %d - %d (%d)\n", $chunk, $start, $start + $remaining - 1, $remaining if $OPTS{'debug'} > 1;

  copyBlocks( \*IN, \*OUT, $chunk, $OPTS{'block-size'}, $start, $remaining );

  close( IN );
  close( OUT );
}

sub copyBlocks {
  my ( $sFH, $dFH, $chunk, $size, $start, $remaining ) = @_;

  my $cksum;
  my $data;
  my $blocks = floor($remaining / $size);
  for my $block (0..$blocks) {
    copyBlock( $sFH, $dFH, $chunk, $blocks, $block, $size, $start, $remaining );
  }
}

sub copyBlock {
  my ( $sFH, $dFH, $chunk, $blocks, $block, $size, $start, $remaining ) = @_;

  my $data;
  my $bstart = $block * $size;
  my $bremaining = $remaining - $bstart;
  $bremaining = $size if $bremaining > $size;
  my $fstart = $start + $bstart;
  my $read = sysread(IN, $data, $bremaining) or next;

  print "  $chunk - $blocks - $fstart - $bstart - $bremaining ($read)\n" if $OPTS{'debug'} > 2;

  syswrite(OUT, $data, $read) if $read;
}

sub execute {
  my ( @args ) = @_;

  my @print = map { ( /\s/ ) ? "\"$_\"" : $_ } @args;
  print join( ' ', @print), "\n" if( $OPTS{'debug'} );

  system( @args );
}

sub isCopied {
  my( $sFH, $dFH, $pos, $size ) = @_;

  return 0 if ( $pos + $size - 1 >= -s $dFH );

  sysseek($sFH, $pos, SEEK_SET);
  sysseek($dFH, $pos, SEEK_SET);

  my $sData;
  my $dData;

  my $sRead = sysread($sFH, $sData, $size);
  my $dRead = sysread($dFH, $dData, $size);

  my $sCksum = md5_hex($sData);
  my $dCksum = md5_hex($dData);

  return ( $sCksum eq $dCksum ) ? 1 : 0;
}


