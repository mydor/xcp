#!/usr/bin/perl
#

use strict;
use File::Basename;
use Parallel::ForkManager;
use POSIX;
use Fcntl;
use Getopt::Long;
use Data::Dumper;
use Digest::MD5 qw(md5_hex);

use constant K => 2**10;
use constant M => 2**20;
use constant G => 2**30;

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
);

my ($src, $dst) = @ARGV;

#my $src = '/mnt/drobo/';
#my $dst = '/mnt/sdb/1/';
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

execute(
   'mkdir',
   '-p',
   $dir
);

sub copyChunk {
  my ( $src, $dst, $chunk, $size ) = @_;

  sysopen( IN, "$src", O_RDONLY );
  sysopen( OUT, "$dst", O_RDWR | O_CREAT );
  binmode(IN);
  binmode(OUT);

  my $start = $chunk * $OPTS{'chunk-size'};
  my $remaining = $size - $start;
  $remaining = $OPTS{'chunk-size'} if $remaining > $OPTS{'chunk-size'};

  #printf "Checking chunk %d at %d - %d\n", $chunk, $start, $start + $remaining - 1 if( $OPTS{'debug'} > 1 );

  if (isCopied( \*IN, \*OUT, $start, $remaining )) {
    printf "SKIPING chunk %d; %d - %d (%d)\n", $chunk, $start, $start + $remaining - 1, $remaining if $OPTS{'debug'} > 1;
    return;
  }

###  printf "Copying chunk at %d - %d\n", $start, $start + $remaining - 1;

### return if ( $start >= -s \*OUT ); # XXX

  sysseek(IN, $start, SEEK_SET);
  sysseek(OUT, $start, SEEK_SET);

  printf "Copying chunk %d; %d - %d (%d)\n", $chunk, $start, $start + $remaining - 1, $remaining if $OPTS{'debug'} > 1;

  copyBlocks( \*IN, \*OUT, $chunk, $OPTS{'block-size'}, $start, $remaining );

  close( IN );
  close( OUT );
}

sub copyBlocks {
  my ( $sFH, $dFH, $chunk, $size, $start, $remaining ) = @_;

### return; # XXX

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

foreach my $chunk ($OPTS{'start-chunk'}..$chunks) {
  my $pid = $pm->start and next;

  copyChunk( $src, "$dst$ext", $chunk, $src_size );

  $pm->finish;
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

$pm->wait_all_children;

