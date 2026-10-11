#!/usr/bin/perl -T
# Jones Code qualified writer observer, protocol 1.
#
# Root-owned, read-only process metadata for the qualified readiness commit.
# Install it root:root, not group/other writable, with every directory up to /
# a real root-owned directory that is not group/other writable, and grant the
# service user exactly one NOPASSWD command shape:
#
#   SERVICE_USER ALL=(root) NOPASSWD: /usr/local/libexec/jones-code/jones-qualified-writer-observer --base CANONICAL_HOME
#
# The only input is the canonical Jones home, owned by the sudo caller. The
# eight state files are fixed. It reports which processes hold those inodes
# open for writing or through a writable shared mapping: file index, PID,
# process birth and access kind. It reads process metadata, not application
# file contents or command lines, and emits no process names or mapped paths.
# It never excludes a process; the launcher excludes
# only its captured trial by PID and birth. Unreadable process evidence ends
# the result as incomplete. It sees descriptor tables and mappings in its own
# PID namespace only; kernel-held references outside them (descriptors in
# flight over a socket, io_uring registered files) are not visible here.
#
# The deadline alarm is armed before any filesystem or /proc access. SIGTERM
# relayed by sudo is left fatal so it also ends killable filesystem waits.
use strict;
use warnings;
use Errno qw(ENOENT ESRCH);
use Fcntl qw(O_RDONLY);

my $HEADER = "jones-qualified-writer-observer 1";
my @STATE_FILES = qw(statev2.sqlite statev2.sqlite-wal statev2.sqlite-shm settings.json keybindings.json server-runtime.json environment-id anonymous-id);
# The launcher waits 15 seconds; both sit inside the server's 30 second exchange.
my $DEADLINE_SECONDS = 10;
my $MAXIMUM_WRITERS = 256;
# Set by the one-shot alarm before it dies. An inner eval may catch that die,
# so observation boundaries and the final result check this flag as well.
our $DEADLINE_EXPIRED = 0;

sub incomplete { die "incomplete $_[0]\n" }

sub deadline_boundary { incomplete("deadline") if $DEADLINE_EXPIRED }

sub absent { return $! == ENOENT || $! == ESRCH }

# Returns undef only when the entry vanished; other failures are incomplete.
sub read_text {
  my ($path) = @_;
  my $handle;
  if (!sysopen($handle, $path, O_RDONLY)) {
    return undef if absent();
    incomplete("read");
  }
  my $text = "";
  while (1) {
    my $read = sysread($handle, $text, 65536, length $text);
    if (!defined $read) {
      return undef if absent();
      incomplete("read");
    }
    return $text if $read == 0;
  }
}

sub entries {
  my ($path) = @_;
  my $handle;
  if (!opendir($handle, $path)) {
    return undef if absent();
    incomplete("directory");
  }
  my @names = map { /\A([0-9]+)\z/ ? $1 : () } readdir($handle);
  closedir($handle);
  return \@names;
}

sub birth {
  my ($process) = @_;
  my $stat = read_text("$process/stat");
  return undef unless defined $stat;
  $stat =~ /\A.*\)\s+(.*)\z/s or incomplete("process-stat");
  my @fields = split " ", $1;
  return $1 if defined $fields[19] && $fields[19] =~ /\A([0-9]+)\z/;
  incomplete("process-stat");
}

# Threads can hold separate descriptor tables, so every task table is read.
sub scan_descriptors {
  my ($process, $targets, $found) = @_;
  my $tasks = entries("$process/task");
  die "gone\n" unless defined $tasks;
  for my $task (@$tasks) {
    my $fds = entries("$process/task/$task/fd");
    next unless defined $fds;
    for my $fd (@$fds) {
      my $link = "$process/task/$task/fd/$fd";
      my @stat = stat($link);
      if (!@stat) {
        next if absent();
        incomplete("descriptor");
      }
      my $index = $targets->{"$stat[0]:$stat[1]"};
      next unless defined $index;
      my $info = read_text("$process/task/$task/fdinfo/$fd");
      if (!defined $info) {
        # A descriptor closed after its inode matched holds nothing now.
        next if !stat($link) && absent();
        incomplete("descriptor-flags");
      }
      $info =~ /^flags:\s+([0-7]+)$/m or incomplete("descriptor-flags");
      my $mode = oct($1) & 3;
      $found->{"$index " . ($mode == 1 ? "write" : "read-write")} = 1 if $mode != 0;
    }
  }
}

# A writable shared mapping keeps write access after its descriptor closes.
# map_files gives the mapped file's own device, which maps text does not on
# every filesystem.
sub scan_mappings {
  my ($process, $targets, $inodes, $found) = @_;
  my $maps = read_text("$process/maps");
  die "gone\n" unless defined $maps;
  for my $line (split /\n/, $maps) {
    $line =~ /\A([0-9a-f]+)-([0-9a-f]+) [r-]([w-])[x-]([sp]) [0-9a-f]+ [0-9a-f]+:[0-9a-f]+ ([0-9]+)(?: .*)?\z/
      or incomplete("mapping-format");
    next unless $3 eq "w" && $4 eq "s" && $inodes->{$5};
    my ($start, $end) = ($1, $2);
    s/\A0+(?=[0-9a-f])// for $start, $end;
    my @stat = stat("$process/map_files/$start-$end");
    if (!@stat) {
      next if absent();
      incomplete("mapping");
    }
    my $index = $targets->{"$stat[0]:$stat[1]"};
    $found->{"$index shared-map"} = 1 if defined $index;
  }
}

sub observe_process {
  my ($process, $pid, $targets, $inodes, $writers) = @_;
  my $birth = birth($process);
  return unless defined $birth;
  my %found;
  my $observed = eval {
    scan_descriptors($process, $targets, \%found);
    scan_mappings($process, $targets, $inodes, \%found);
    1;
  };
  my $error = $@;
  # This eval would otherwise swallow the alarm when the process also vanished.
  deadline_boundary();
  my $after = birth($process);
  # Missing evidence is harmless only when the observed process vanished.
  return unless defined $after;
  incomplete("process-changed") if $after ne $birth;
  die($error eq "gone\n" ? "incomplete process-evidence\n" : $error) unless $observed;
  for my $key (keys %found) {
    my ($index, $access) = split / /, $key;
    $writers->{"$index $pid $birth $access"} = 1;
  }
  incomplete("writer-limit") if scalar(keys %$writers) > $MAXIMUM_WRITERS;
}

sub observe {
  my ($proc, $files) = @_;
  my (%targets, %inodes, %seen, %writers);
  for my $file (@$files) {
    $targets{"$file->{dev}:$file->{ino}"} = $file->{index};
    $inodes{$file->{ino}} = 1;
  }
  my $processes = 0;
  # A second listing covers processes started during the first.
  for my $discovery (1, 2) {
    my $pids = entries($proc);
    incomplete("process-list") unless defined $pids;
    for my $pid (@$pids) {
      deadline_boundary();
      next if $seen{$pid}++;
      $processes++;
      observe_process("$proc/$pid", $pid, \%targets, \%inodes, \%writers);
    }
  }
  deadline_boundary();
  return (\%writers, $processes);
}

sub emit_observation {
  my ($writers, $processes) = @_;
  my @lines = sort {
    my @left = split / /, $a;
    my @right = split / /, $b;
    $left[0] <=> $right[0] || $left[1] <=> $right[1] || $left[3] cmp $right[3]
  } keys %$writers;
  return join("", map { "writer $_\n" } @lines) . "complete $processes\n";
}

sub reject {
  print "rejected $_[0]\n";
  exit 2;
}

# Anyone able to rename an ancestor could substitute this file, so the helper
# and every directory up to / must be real, root-owned and not group/other
# writable.
sub trusted_self {
  my ($path) = $0 =~ m{\A((?:/[A-Za-z0-9._@+-]{1,255}){1,64})\z} or return 0;
  return 0 if grep { $_ eq "." || $_ eq ".." } split m{/}, $path;
  my @file = lstat($path);
  return 0 unless @file && -f _ && $file[4] == 0 && !($file[2] & 022);
  my $directory = $path;
  while ($directory =~ s{/[^/]+\z}{}) {
    my @stat = lstat(length $directory ? $directory : "/");
    return 0 unless @stat && -d _ && !-l _ && $stat[4] == 0 && !($stat[2] & 022);
    last unless length $directory;
  }
  return 1;
}

# Returns (exit code, output); any failure, including the alarm, is typed.
sub bounded {
  my ($seconds, $work) = @_;
  local $DEADLINE_EXPIRED = 0;
  local $SIG{ALRM} = sub {
    $DEADLINE_EXPIRED = 1;
    die "incomplete deadline\n";
  };
  alarm($seconds);
  my $output = eval { $work->() };
  my $error = $@;
  alarm(0);
  # A swallowed alarm can never yield a complete result.
  return (3, "incomplete deadline\n") if $DEADLINE_EXPIRED;
  return (0, $output) if defined $output;
  return (3, $error =~ /\A(incomplete [a-z-]+\n)\z/ ? $1 : "incomplete internal\n");
}

sub run {
  print "$HEADER\n";
  # The alarm covers request validation as well as the /proc observation.
  my ($code, $output) = bounded($DEADLINE_SECONDS, \&observe_request);
  print STDOUT $output;
  exit $code;
}

# Rejections exit directly; observation failures die as "incomplete <reason>".
sub observe_request {
  reject("not-root") unless $> == 0 && $< == 0;
  my ($uid) = (defined $ENV{SUDO_UID} ? $ENV{SUDO_UID} : "") =~ /\A([1-9][0-9]{0,9})\z/
    or reject("caller");
  reject("arguments") unless @ARGV == 2 && $ARGV[0] eq "--base";
  my ($base) = $ARGV[1] =~ m{\A((?:/[A-Za-z0-9._@+-]{1,255}){1,64})\z} or reject("base");
  reject("base") if grep { $_ eq "." || $_ eq ".." } split m{/}, $base;
  reject("helper-ownership") unless trusted_self();
  # Every component is a real directory, so the home is canonical.
  my $prefix = "";
  for my $component (grep { length } split m{/}, $base) {
    $prefix .= "/$component";
    my @stat = lstat($prefix);
    reject("base") unless @stat && -d _ && !-l _;
  }
  my @home = lstat($base);
  reject("base-owner") unless $home[4] == $uid;
  my $userdata = "$base/userdata";
  my @data = lstat($userdata);
  reject("userdata") unless @data && -d _ && !-l _ && $data[4] == $uid;
  my @files;
  for my $index (0 .. $#STATE_FILES) {
    my @stat = lstat("$userdata/$STATE_FILES[$index]");
    if (!@stat) {
      next if $! == ENOENT;
      reject("state-file");
    }
    reject("state-file") unless -f _ && $stat[4] == $uid;
    push @files, { index => $index, dev => $stat[0], ino => $stat[1] };
    print "file $index $stat[0] $stat[1]\n";
  }
  return emit_observation(observe("/proc", \@files));
}

run() unless caller;
1;
