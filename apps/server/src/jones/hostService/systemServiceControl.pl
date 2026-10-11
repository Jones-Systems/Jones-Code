#!/usr/bin/perl -T
# Jones Code system service control, protocol 1.
#
# Root-owned worker for one existing system unit that runs a Jones home as its
# service user. Install it root:root at
# /usr/local/libexec/jones-code/jones-system-service-control, not group or
# other writable, with every directory up to / a real root-owned directory that
# is not group or other writable. The root-owned policy file
# /etc/jones-code/system-service.policy fixes the unit, user, UID, GID, home,
# slice and retained serve executable. No sudoers rule is installed: the owner
# authenticates sudo for each bootstrap, upgrade or recovery session.
#
# Operations take only expected preimages, never units, paths, commands or
# environment:
#
#   inspect
#   stop --expect-unit SHA --expect-owned absent|SHA --expect-main-pid PID --expect-main-birth TICKS
#   install-launcher --expect-unit SHA --expect-owned absent|SHA --launcher-version VERSION
#   start --expect-unit SHA --expect-owned absent|SHA
#   archive-launcher --expect-unit SHA --expect-owned SHA
#
# The unit preimage is the fragment plus every drop-in other than the one
# owned file, recorded by owner, mode and SHA-256. Every mutation compares it
# and the owned file first. Only the owned drop-in is ever written; it is
# rendered here from the policy home, a strict version and the host, port and
# mode of the retained serve command, staged in a root-only file beside it and
# published by link or rename. An unpublished stage is removed on refusal,
# error or interruption while it is still the same inode; one that cannot be
# removed that way stays as evidence and the result is unknown. Output carries
# metadata and digests,
# never unit contents, Environment values or an unexpected command line.
#
# Results: `complete` or `no_change` (exit 0), `rejected REASON` before any
# effect (exit 2), `unknown REASON` once an effect may have started (exit 3).
# The protocol header is flushed before any work, so its absence proves that
# this worker did not run.
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use Errno qw(ENOENT EEXIST);
use Fcntl qw(O_RDONLY O_WRONLY O_CREAT O_EXCL O_NOFOLLOW O_DIRECTORY);
use IO::Handle;
use POSIX ();

our $HEADER = "jones-system-service-control 1";
our $POLICY_PATH = "/etc/jones-code/system-service.policy";
our $SYSTEMD_DIRECTORY = "/etc/systemd/system";
our $ARCHIVE_DIRECTORY = "/var/lib/jones-code/system-service-archive";
our $PROC = "/proc";
our $CGROUP_ROOT = "/sys/fs/cgroup";
our $SYSTEMCTL = "/usr/bin/systemctl";
# Fixture harnesses substitute their own UID; installed runs keep root.
our $ROOT_UID = 0;
our $OWNED_NAME = "zzzz-jones-qualified-launcher.conf";
our $OWNED_MARKER = "# Jones qualified system launcher protocol 1";
# Covers a unit's default 90 second stop timeout and its final kill.
our $DEADLINE_SECONDS = 150;
our $OUTPUT_LIMIT = 65536;

my $PATH = qr{\A((?:/[A-Za-z0-9._@+-]{1,255}){1,64})\z};
my $SHA = qr{\A([a-f0-9]{64})\z};
my $VERSION = qr{\A([0-9]{1,9}\.[0-9]{1,9}\.[0-9]{1,9}-preview\.[0-9]{8}\.[0-9]{1,20}(?:\.[0-9]{1,20})?)\z};
my @SHOW = qw(FragmentPath DropInPaths User Group NoNewPrivileges KillMode Slice ControlGroup ActiveState SubState MainPID ExecStart);

# Set immediately before the first systemctl mutation or owned-file rename,
# link or unlink; any later refusal is no longer proof of no effect.
our $EFFECT_STARTED = 0;

# Entries this invocation created before publishing anything: the owned
# drop-in's staging file and, when absent, the drop-in directory. Each is
# removed only while the same inode is still at the same path; anything else
# stays as evidence and the result becomes unknown.
our @CREATED;

sub release_created {
  my @kept;
  for my $entry (reverse @CREATED) {
    my @stat = lstat($entry->{path});
    my $same = @stat && !-l _ && $stat[0] == $entry->{dev} && $stat[1] == $entry->{ino};
    my $removed =
        !$same ? 0
      : $entry->{kind} eq "file" ? -f _ && unlink($entry->{path})
      : -d _ && rmdir($entry->{path});
    push @kept, $entry unless $removed;
  }
  @CREATED = reverse @kept;
  return !@CREATED;
}

sub reject {
  die "unknown $_[0]\n" if $EFFECT_STARTED;
  die "unknown created-cleanup\n" if @CREATED && !release_created();
  print "rejected $_[0]\n";
  exit 2;
}
sub unknown { die "unknown $_[0]\n" }
sub emit { print "$_[0]\n" }

sub canonical {
  my ($path) = @_;
  my ($clean) = (defined $path ? $path : "") =~ $PATH or return undef;
  return undef if grep { $_ eq "." || $_ eq ".." } split m{/}, $clean;
  return $clean;
}

# A real entry of the expected kind, owned by root and not group/other writable.
sub trusted_entry {
  my ($path, $kind) = @_;
  my @stat = lstat($path);
  return 0 unless @stat && !-l _;
  return 0 unless $kind eq "file" ? -f _ : -d _;
  return $stat[4] == $ROOT_UID && !($stat[2] & 022) ? 1 : 0;
}

# Anyone able to rename an ancestor could substitute the entry.
sub trusted_chain {
  my ($path) = @_;
  my $directory = $path;
  while ($directory =~ s{/[^/]+\z}{}) {
    return 0 unless trusted_entry(length $directory ? $directory : "/", "directory");
    last unless length $directory;
  }
  return 1;
}

sub trusted_file { return trusted_entry($_[0], "file") && trusted_chain($_[0]) }

sub trusted_self {
  my $path = canonical($0);
  return defined $path && trusted_file($path);
}

# Returns undef only when the file is absent.
sub read_small {
  my ($path, $limit) = @_;
  my $handle;
  if (!sysopen($handle, $path, O_RDONLY | O_NOFOLLOW)) {
    return undef if $! == ENOENT;
    reject("read");
  }
  my @stat = stat($handle);
  reject("read") unless @stat && -f _ && $stat[7] <= $limit;
  my $text = "";
  while (1) {
    my $read = sysread($handle, $text, 65536, length $text);
    reject("read") unless defined $read;
    last if $read == 0;
    reject("read") if length $text > $limit;
  }
  close($handle);
  return $text;
}

sub read_policy {
  reject("policy-trust") unless trusted_file($POLICY_PATH);
  my $text = read_small($POLICY_PATH, 4096);
  reject("policy") unless defined $text && $text !~ /[^\x0a\x20-\x7e]/;
  my @lines = split /\n/, $text, -1;
  my @keys = qw(unit user uid gid base slice retained-entry);
  reject("policy")
    unless @lines == @keys + 2 && shift(@lines) eq "jones-system-service-policy 1" && pop(@lines) eq "";
  my %policy;
  for my $index (0 .. $#keys) {
    my ($key, $value) = $lines[$index] =~ /\A([a-z-]+) (\S+)\z/ or reject("policy");
    reject("policy") unless $key eq $keys[$index];
    $policy{$key} = $value;
  }
  my %clean;
  ($clean{unit}) = $policy{unit} =~ /\A([A-Za-z0-9][A-Za-z0-9_.-]{0,200}\.service)\z/ or reject("policy");
  ($clean{user}) = $policy{user} =~ /\A([a-z_][a-z0-9_-]{0,31})\z/ or reject("policy");
  ($clean{uid}) = $policy{uid} =~ /\A([1-9][0-9]{0,9})\z/ or reject("policy");
  ($clean{gid}) = $policy{gid} =~ /\A([1-9][0-9]{0,9})\z/ or reject("policy");
  ($clean{slice}) = $policy{slice} =~ /\A([A-Za-z0-9][A-Za-z0-9_.-]{0,200}\.slice)\z/ or reject("policy");
  $clean{base} = canonical($policy{base}) // reject("policy");
  $clean{"retained-entry"} = canonical($policy{"retained-entry"}) // reject("policy");
  reject("policy") if $clean{user} eq "root";
  my @account = getpwnam($clean{user});
  reject("policy-user") unless @account && $account[2] == $clean{uid} && $account[3] == $clean{gid};
  $clean{group} = (getgrgid($clean{gid}))[0] // reject("policy-user");
  $clean{sha256} = sha256_hex($text);
  return \%clean;
}

sub operation_arguments {
  my %shapes = (
    "inspect" => [],
    "stop" => [qw(expect-unit expect-owned expect-main-pid expect-main-birth)],
    "install-launcher" => [qw(expect-unit expect-owned launcher-version)],
    "start" => [qw(expect-unit expect-owned)],
    "archive-launcher" => [qw(expect-unit expect-owned)],
  );
  my ($operation, @rest) = @ARGV;
  reject("arguments") unless defined $operation && exists $shapes{$operation};
  my @keys = @{ $shapes{$operation} };
  reject("arguments") unless @rest == 2 * @keys;
  my %values;
  for my $index (0 .. $#keys) {
    reject("arguments") unless $rest[2 * $index] eq "--$keys[$index]";
    my $value = $rest[2 * $index + 1];
    my $key = $keys[$index];
    my ($clean) =
        $key eq "expect-unit" ? $value =~ $SHA
      : $key eq "expect-owned" ? ($value eq "absent" ? ("absent") : $value =~ $SHA)
      : $key eq "expect-main-pid" ? $value =~ /\A([1-9][0-9]{0,9})\z/
      : $key eq "expect-main-birth" ? $value =~ /\A([0-9]{1,20})\z/
      : $value =~ $VERSION;
    reject("arguments") unless defined $clean;
    $values{$key} = $clean;
  }
  reject("arguments") if $operation eq "archive-launcher" && $values{"expect-owned"} eq "absent";
  my ($clean_operation) = $operation =~ /\A([a-z-]+)\z/;
  return ($clean_operation, \%values);
}

# Runs /usr/bin/systemctl with an empty environment, no input and bounded
# output. Fixture harnesses replace this sub.
sub systemctl {
  my @args = @_;
  pipe(my $read, my $write) or unknown("pipe");
  my $pid = fork();
  unknown("fork") unless defined $pid;
  if ($pid == 0) {
    close($read);
    open(STDIN, "<", "/dev/null") or POSIX::_exit(127);
    open(STDOUT, ">&", $write) or POSIX::_exit(127);
    open(STDERR, ">", "/dev/null") or POSIX::_exit(127);
    exec { $SYSTEMCTL } $SYSTEMCTL, "--no-pager", @args or POSIX::_exit(127);
  }
  close($write);
  my $output = "";
  while (1) {
    my $count = sysread($read, $output, 65536, length $output);
    unknown("systemctl-output") unless defined $count;
    last if $count == 0;
    if (length $output > $OUTPUT_LIMIT) {
      kill("KILL", $pid);
      waitpid($pid, 0);
      unknown("systemctl-output");
    }
  }
  close($read);
  waitpid($pid, 0);
  return ($? == 0, $output);
}

sub show {
  my ($policy) = @_;
  my ($ok, $output) = systemctl("show", $policy->{unit}, "--property=" . join(",", @SHOW));
  reject("show") unless $ok;
  my %values;
  for my $line (split /\n/, $output) {
    my ($key, $value) = $line =~ /\A([A-Za-z]+)=(.*)\z/ or reject("show");
    reject("show") if exists $values{$key} || !grep { $_ eq $key } @SHOW;
    $values{$key} = $value;
  }
  reject("show") unless scalar(keys %values) == scalar(@SHOW);
  return \%values;
}

# Fragment and non-owned drop-ins in systemd order, with their digest records.
# Only install-launcher's second scan passes `$staged`, the exact staging file
# it created, matched by name and inode; every other entry, including a staging
# file left by an interrupted run, is checked as usual.
sub unit_preimage {
  my ($policy, $staged) = @_;
  my $fragment = "$SYSTEMD_DIRECTORY/$policy->{unit}";
  my $directory = "$fragment.d";
  reject("unit-trust") unless trusted_file($fragment);
  my @records = ("jones-system-unit-digest 1");
  my @texts;
  my $record = sub {
    my ($label, $path) = @_;
    my @stat = lstat($path);
    my $text = read_small($path, 65536);
    reject("unit-trust") unless defined $text;
    push @texts, $text;
    push @records, sprintf("%s %d %04o %s", $label, $stat[4], $stat[2] & 07777, sha256_hex($text));
  };
  $record->("fragment", $fragment);
  my @names;
  my $owned;
  if (lstat($directory)) {
    reject("unit-trust") unless trusted_entry($directory, "directory");
    opendir(my $handle, $directory) or reject("unit-trust");
    @names = sort grep { $_ ne "." && $_ ne ".." } readdir($handle);
    closedir($handle);
  } elsif ($! != ENOENT) {
    reject("unit-trust");
  }
  my @dropins;
  for my $raw (@names) {
    if (defined $staged && $raw eq $staged->{name}) {
      my @stat = lstat("$directory/$raw");
      next
        if @stat && -f _ && !-l _ && $stat[0] == $staged->{dev} && $stat[1] == $staged->{ino};
    }
    my ($name) = $raw =~ /\A([A-Za-z0-9][A-Za-z0-9_.@-]{0,200}\.conf)\z/ or reject("unit-dropin");
    reject("unit-trust") unless trusted_entry("$directory/$name", "file");
    if ($name eq $OWNED_NAME) {
      $owned = sha256_hex(read_small("$directory/$name", 65536) // reject("unit-trust"));
      next;
    }
    reject("unit-precedence") if ($name cmp $OWNED_NAME) > 0;
    push @dropins, $name;
    $record->("dropin $name", "$directory/$name");
  }
  return {
    fragment => $fragment,
    directory => $directory,
    dropins => \@dropins,
    texts => \@texts,
    owned => $owned // "absent",
    digest => sha256_hex(join("\n", @records) . "\n"),
    records => [@records[1 .. $#records]],
  };
}

# The single effective ExecStart of the non-owned files must be the policy's
# retained executable running `serve` with only known loopback flags.
sub retained_serve {
  my ($policy, $preimage) = @_;
  my @commands;
  for my $text (@{ $preimage->{texts} }) {
    my $section = "";
    for my $line (split /\n/, $text) {
      reject("unit-grammar") if $line =~ /[\x00-\x08\x0a-\x1f\x7f]/ || $line =~ /\\\z/;
      my ($trimmed) = $line =~ /\A\s*(.*?)\s*\z/;
      next if $trimmed eq "" || $trimmed =~ /\A[#;]/;
      if ($trimmed =~ /\A\[([A-Za-z]+)\]\z/) {
        $section = $1;
        next;
      }
      next unless $section eq "Service";
      my ($key, $value) = $trimmed =~ /\A([A-Za-z]+)\s*=\s*(.*)\z/ or reject("unit-grammar");
      next unless $key eq "ExecStart";
      if ($value eq "") {
        @commands = ();
      } else {
        push @commands, $value;
      }
    }
  }
  reject("unit-exec") unless @commands == 1;
  my @argv = split / +/, $commands[0];
  reject("unit-exec") if grep { !m{\A[A-Za-z0-9._@+/-]+\z} } @argv;
  my $command = join(" ", @argv);
  my $executable = shift @argv;
  reject("unit-exec") unless $executable eq $policy->{"retained-entry"};
  reject("unit-exec") unless @argv && shift(@argv) eq "serve" && @argv % 2 == 0;
  my %flags;
  while (@argv) {
    my ($flag, $value) = splice(@argv, 0, 2);
    my ($name) = $flag =~ /\A--(host|port|base-dir|mode)\z/ or reject("unit-exec");
    reject("unit-exec") if exists $flags{$name};
    $flags{$name} = $value;
  }
  my %clean;
  if (exists $flags{"base-dir"}) {
    reject("unit-exec") unless $flags{"base-dir"} eq $policy->{base};
  }
  if (exists $flags{port}) {
    ($clean{port}) = $flags{port} =~ /\A([1-9][0-9]{0,4})\z/ or reject("unit-exec");
    reject("unit-exec") if $clean{port} > 65535;
  }
  if (exists $flags{host}) {
    ($clean{host}) = $flags{host} =~ /\A((?:[0-9]{1,3}\.){3}[0-9]{1,3}|localhost)\z/ or reject("unit-exec");
  }
  if (exists $flags{mode}) {
    ($clean{mode}) = $flags{mode} =~ /\A(web|desktop)\z/ or reject("unit-exec");
  }
  # Compared with systemd's effective argv only to classify it, never emitted.
  ($clean{command}) = $command =~ /\A(.*)\z/;
  return \%clean;
}

sub launcher_entry {
  my ($policy, $version) = @_;
  return "$policy->{base}/runtime/versions/$version/t3";
}

sub render_owned {
  my ($policy, $serve, $version) = @_;
  my @lines = (
    $OWNED_MARKER,
    "[Service]",
    "ExecStart=",
    "ExecStart=" . launcher_entry($policy, $version) . " __service-launcher",
    "Environment=T3CODE_HOME=$policy->{base}",
  );
  push @lines, "Environment=T3CODE_PORT=$serve->{port}" if exists $serve->{port};
  push @lines, "Environment=T3CODE_HOST=$serve->{host}" if exists $serve->{host};
  push @lines, "Environment=T3CODE_MODE=$serve->{mode}" if exists $serve->{mode};
  return join("\n", @lines) . "\n";
}

sub exec_kind {
  my ($policy, $serve, $show) = @_;
  my ($argv) = $show->{ExecStart} =~ /argv\[\]=([^;]*?) ;/ or return "other";
  $argv =~ s/ +\z//;
  return "retained" if $argv eq $serve->{command};
  my $prefix = quotemeta("$policy->{base}/runtime/versions/");
  if ($argv =~ /\A$prefix([^\/ ]+)\/t3 __service-launcher\z/) {
    my ($version) = $1 =~ $VERSION;
    return "launcher $version" if defined $version;
  }
  return "other";
}

# The service identity this worker preserves: run as the policy user, never
# root, with NoNewPrivileges, in the policy slice, only from this unit's own
# fragment and drop-in directory.
sub verified_show {
  my ($policy, $preimage) = @_;
  my $show = show($policy);
  reject("unit-fragment") unless $show->{FragmentPath} eq $preimage->{fragment};
  my %present = map { ("$preimage->{directory}/$_" => 1) } @{ $preimage->{dropins} };
  $present{"$preimage->{directory}/$OWNED_NAME"} = 1 if $preimage->{owned} ne "absent";
  my @paths = grep { length } split / /, $show->{DropInPaths};
  reject("unit-dropin-path")
    unless scalar(@paths) == scalar(keys %present) && !grep { !$present{$_} } @paths;
  reject("unit-user") unless $show->{User} eq $policy->{user};
  reject("unit-user") unless $show->{Group} eq "" || $show->{Group} eq $policy->{group};
  reject("unit-hardening") unless $show->{NoNewPrivileges} eq "yes";
  reject("unit-slice") unless $show->{Slice} eq $policy->{slice};
  reject("unit-state") unless $show->{MainPID} =~ /\A([0-9]{1,10})\z/;
  return $show;
}

sub process_birth {
  my ($pid) = @_;
  my $stat = read_small("$PROC/$pid/stat", 4096);
  return undef unless defined $stat;
  my ($fields) = $stat =~ /\A.*\)\s+(.*)\z/s or return undef;
  my @fields = split " ", $fields;
  return defined $fields[19] && $fields[19] =~ /\A([0-9]+)\z/ ? $1 : undef;
}

# The main process belongs to this unit's cgroup and runs as the policy UID.
sub process_bound {
  my ($policy, $show, $pid) = @_;
  my $cgroup = read_small("$PROC/$pid/cgroup", 65536);
  my $status = read_small("$PROC/$pid/status", 65536);
  return 0 unless defined $cgroup && defined $status;
  return 0 unless grep { $_ eq "0::$show->{ControlGroup}" } split /\n/, $cgroup;
  my ($uids) = $status =~ /^Uid:\s+([0-9\s]+)$/m or return 0;
  my @uids = split " ", $uids;
  return @uids == 4 && !grep { $_ != $policy->{uid} } @uids;
}

sub cgroup_empty {
  my ($show) = @_;
  my ($group) = $show->{ControlGroup} =~ m{\A((?:/[A-Za-z0-9._\@:\\-]+)+)\z} or return 0;
  my $procs = read_small("$CGROUP_ROOT$group/cgroup.procs", 1048576);
  return !defined $procs || $procs eq "";
}

sub compare_and_hold {
  my ($policy, $values) = @_;
  my $preimage = unit_preimage($policy);
  reject("unit-changed") unless $preimage->{digest} eq $values->{"expect-unit"};
  reject("owned-changed") unless $preimage->{owned} eq $values->{"expect-owned"};
  my $serve = retained_serve($policy, $preimage);
  my $show = verified_show($policy, $preimage);
  return ($preimage, $serve, $show);
}

sub sync_directory {
  my ($path) = @_;
  sysopen(my $handle, $path, O_RDONLY | O_DIRECTORY) or unknown("sync");
  $handle->sync or unknown("sync");
  close($handle);
}

sub report {
  my ($policy, $preimage, $serve, $show) = @_;
  emit("policy $policy->{sha256}");
  emit("unit-digest $preimage->{digest}");
  emit("owned $preimage->{owned}");
  emit($_) for @{ $preimage->{records} };
  my ($active) = "$show->{ActiveState} $show->{SubState}" =~ /\A([a-z-]+ [a-z-]+)\z/;
  emit("active " . ($active // "unknown unknown"));
  emit("main-pid $show->{MainPID}");
  if ($show->{MainPID} ne "0") {
    my $birth = process_birth($show->{MainPID});
    emit("main-birth $birth") if defined $birth && process_bound($policy, $show, $show->{MainPID});
  }
  my ($group) = $show->{ControlGroup} =~ m{\A((?:/[A-Za-z0-9._\@:\\-]+)+)\z};
  emit("control-group $group") if defined $group;
  my ($kill) = $show->{KillMode} =~ /\A([a-z-]+)\z/;
  emit("kill-mode " . ($kill // "unknown"));
  emit("exec " . exec_kind($policy, $serve, $show));
  emit("serve-$_ $serve->{$_}") for grep { exists $serve->{$_} } qw(port host mode);
}

sub stop {
  my ($policy, $values) = @_;
  my ($preimage, $serve, $show) = compare_and_hold($policy, $values);
  my $pid = $values->{"expect-main-pid"};
  reject("main-process")
    unless $show->{ActiveState} eq "active" && $show->{MainPID} eq $pid
    && (process_birth($pid) // "") eq $values->{"expect-main-birth"}
    && process_bound($policy, $show, $pid);
  $EFFECT_STARTED = 1;
  my ($ok) = systemctl("stop", $policy->{unit});
  unknown("stop") unless $ok;
  my $after = show($policy);
  # An inactive unit reports no cgroup, so the running unit's path is checked.
  unknown("stop-readback")
    unless $after->{ActiveState} eq "inactive" && $after->{MainPID} eq "0" && cgroup_empty($show);
  report($policy, unit_preimage($policy), $serve, $after);
}

sub install_launcher {
  my ($policy, $values) = @_;
  my ($preimage, $serve, $show) = compare_and_hold($policy, $values);
  reject("unit-active") unless $show->{ActiveState} eq "inactive" && $show->{MainPID} eq "0";
  my $version = $values->{"launcher-version"};
  my @entry = lstat(launcher_entry($policy, $version));
  reject("launcher-entry")
    unless @entry && -f _ && !-l _ && $entry[4] == $policy->{uid} && !($entry[2] & 022);
  my $text = render_owned($policy, $serve, $version);
  my $owned = "$preimage->{directory}/$OWNED_NAME";
  if ($values->{"expect-owned"} ne sha256_hex($text)) {
    if (!lstat($preimage->{directory})) {
      reject("unit-trust") unless $! == ENOENT;
      mkdir($preimage->{directory}, 0755) or reject("unit-trust");
      my @created = lstat($preimage->{directory});
      unknown("unit-directory") unless @created;
      push @CREATED,
        { path => $preimage->{directory}, dev => $created[0], ino => $created[1], kind => "directory" };
    }
    # The staging file is root-only until it is published; its identity is
    # taken from the descriptor that created it, so only that inode is ever
    # skipped by the second scan or removed again.
    my $name = ".$OWNED_NAME.$$";
    my $temporary = "$preimage->{directory}/$name";
    sysopen(my $handle, $temporary, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600) or reject("owned-write");
    my @stage = stat($handle);
    if (!@stage) {
      close($handle);
      unknown("owned-write");
    }
    push @CREATED, { path => $temporary, dev => $stage[0], ino => $stage[1], kind => "file" };
    my $written = syswrite($handle, $text);
    my $synced = defined $written && $written == length($text) && $handle->sync;
    close($handle);
    reject("owned-write") unless $synced;
    my $current = unit_preimage($policy, { name => $name, dev => $stage[0], ino => $stage[1] });
    reject("unit-changed")
      if $current->{digest} ne $preimage->{digest} || $current->{owned} ne $values->{"expect-owned"};
    if ($values->{"expect-owned"} eq "absent") {
      # A failed link publishes nothing, so the refusal removes only the stage.
      if (!link($temporary, $owned)) {
        reject($! == EEXIST ? "owned-changed" : "owned-write");
      }
      $EFFECT_STARTED = 1;
      unlink($temporary) or unknown("owned-write");
    } else {
      $EFFECT_STARTED = 1;
      rename($temporary, $owned) or unknown("owned-write");
    }
    # Published: the stage no longer exists and the directory now holds the
    # owned drop-in, so nothing created here is removed afterwards.
    @CREATED = ();
    sync_directory($preimage->{directory});
  }
  $EFFECT_STARTED = 1;
  my ($reloaded) = systemctl("daemon-reload");
  unknown("daemon-reload") unless $reloaded;
  my $after = unit_preimage($policy);
  unknown("owned-readback") unless $after->{digest} eq $preimage->{digest} && $after->{owned} eq sha256_hex($text);
  my $readback = verified_show($policy, $after);
  unknown("launcher-readback") unless exec_kind($policy, $serve, $readback) eq "launcher $version";
  unknown("hardening-readback")
    unless $readback->{KillMode} eq $show->{KillMode} && $readback->{ControlGroup} eq $show->{ControlGroup};
  report($policy, $after, $serve, $readback);
}

sub start {
  my ($policy, $values) = @_;
  my ($preimage, $serve, $show) = compare_and_hold($policy, $values);
  reject("unit-active") unless $show->{ActiveState} eq "inactive" && $show->{MainPID} eq "0";
  $EFFECT_STARTED = 1;
  my ($ok) = systemctl("start", $policy->{unit});
  unknown("start") unless $ok;
  my $after = verified_show($policy, unit_preimage($policy));
  unknown("start-readback") unless $after->{ActiveState} eq "active" && $after->{MainPID} ne "0";
  report($policy, unit_preimage($policy), $serve, $after);
}

sub archive_launcher {
  my ($policy, $values) = @_;
  my ($preimage, $serve, $show) = compare_and_hold($policy, $values);
  reject("unit-active") unless $show->{ActiveState} eq "inactive" && $show->{MainPID} eq "0";
  my $parent = $ARCHIVE_DIRECTORY;
  $parent =~ s{/[^/]+\z}{};
  for my $directory ($parent, $ARCHIVE_DIRECTORY) {
    if (!lstat($directory)) {
      reject("archive-trust") unless $! == ENOENT;
      mkdir($directory, $directory eq $parent ? 0755 : 0700) or reject("archive-trust");
    }
    reject("archive-trust") unless trusted_entry($directory, "directory");
  }
  reject("archive-trust") unless trusted_chain($ARCHIVE_DIRECTORY);
  my $target = "$ARCHIVE_DIRECTORY/$values->{'expect-owned'}-" . time() . ".conf";
  reject("archive-occupied") if lstat($target) || $! != ENOENT;
  my $owned = "$preimage->{directory}/$OWNED_NAME";
  reject("owned-changed") unless sha256_hex(read_small($owned, 65536) // "") eq $values->{"expect-owned"};
  link($owned, $target) or reject("archive-write");
  $EFFECT_STARTED = 1;
  sync_directory($ARCHIVE_DIRECTORY);
  unlink($owned) or unknown("archive-write");
  sync_directory($preimage->{directory});
  my ($reloaded) = systemctl("daemon-reload");
  unknown("daemon-reload") unless $reloaded;
  my $after = unit_preimage($policy);
  unknown("archive-readback") unless $after->{digest} eq $preimage->{digest} && $after->{owned} eq "absent";
  my $readback = verified_show($policy, $after);
  unknown("archive-readback") unless exec_kind($policy, $serve, $readback) eq "retained";
  report($policy, $after, $serve, $readback);
}

sub request {
  reject("not-root") unless $> == $ROOT_UID && $< == $ROOT_UID;
  reject("worker-ownership") unless trusted_self();
  my $policy = read_policy();
  my $caller = defined $ENV{SUDO_UID} ? $ENV{SUDO_UID} : "";
  reject("caller") unless $caller eq $policy->{uid};
  %ENV = ();
  my ($operation, $values) = operation_arguments();
  if ($operation eq "inspect") {
    my $preimage = unit_preimage($policy);
    my $serve = retained_serve($policy, $preimage);
    report($policy, $preimage, $serve, verified_show($policy, $preimage));
  } elsif ($operation eq "stop") {
    stop($policy, $values);
  } elsif ($operation eq "install-launcher") {
    install_launcher($policy, $values);
  } elsif ($operation eq "start") {
    start($policy, $values);
  } else {
    archive_launcher($policy, $values);
  }
  return "complete";
}

sub run {
  STDOUT->autoflush(1);
  print "$HEADER\n";
  my $terminal = eval {
    local $SIG{ALRM} = sub { die "unknown deadline\n" };
    # sudo relays these from the caller's deadline or cancellation; the
    # unwinding below still removes this run's unpublished staging.
    local $SIG{TERM} = sub { die "unknown interrupted\n" };
    local $SIG{INT} = sub { die "unknown interrupted\n" };
    local $SIG{HUP} = sub { die "unknown interrupted\n" };
    alarm($DEADLINE_SECONDS);
    my $result = request();
    alarm(0);
    $result;
  };
  alarm(0);
  if (defined $terminal) {
    print "$terminal\n";
    exit 0;
  }
  my $error = $@;
  # The result stays unknown whether or not this removes anything; whatever
  # cannot be removed by its exact identity is left as evidence.
  release_created() if @CREATED;
  print $error =~ /\A(unknown [a-z-]+\n)\z/ ? $1 : "unknown internal\n";
  exit 3;
}

run() unless caller;
1;
