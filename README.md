# muscle

(This project is experimental. It works for me. It may not work for you.)

The project consists of musclefs, snapshotsfs, and muscle. The former
two are 9P file servers, S3-backed, with a git-like backend allowing
for file system history and file system merge. musclefs is for the
current revision of the filesystem, snapshotsfs is for looking at any
past revision.  Revisions are linked together in a git-like history.
The latter is a CLI tool that offers additional operations on the same
data that is exposed via the file server.

Later sections will go more in-depth, but here is an overview of the features.

The storage backend of the file system consists of many layers, but is
ultimately backed by cloud storage. (That would be S3 at the time of
writing, but more can be added.) This makes it possible to expose and
operate on the same data in all computers. In particular, it makes it
possible to have that same data in a freshly installed machine, loading
data from the cloud when necessary.

The file system can often be used without a persistent internet
connection, as data is stored locally as well. A local store is paired
with the remote store, essentially acting as a write-back cache. There is
no automatic expiration of items from the cache, but it can be trivially
be achieved with `find` and, say, `-atime +30` test, for blobs last
accessed more than a month ago. (I won't provide exact commands as what
needs to be escaped depends on the shell.)

The file system supports taking incremental snapshots, called revisions,
which are linked, in such a way that you can have a history of snapshots
akin to a git history of commits. In particular, it is possible to see
the diff between any two revisions of the file system.  For example,
showing difference between last two revisions for the tree named plank:

```
% muscle history -d -n 1 plank
revision taken 2m50s ago, precisely 2019-06-24 03:46:49 +0100 IST
host plank.localdomain
key f615760c8733c73cbd29979c80739cc1dffd2358efd8fc5c3683b8480760b4a6
parents 10b5dec073b47914c98d6c4c7b17671f555b0e62e75ab7db1b4e574f83a565c0
root 5fb88bac5d2e83d7d91691512225159ffdb9bac42303ae6dab48f60af84672dc
comment

... some output concerning parent dirs of go.mod removed ...
--- a/root/sources/muscle/go.mod
+++ b/root/sources/muscle/go.mod
-Key 89d6252f5caae01bf24b6d7ad53014697302ef8d854fc6b9040b2a8299dbe5ce
+Key c5e7edad8063ca89031b5508f80d2fbdee0ad2656e2e0641593f79a6ecbe2634
 Dir.Size 0
 Dir.Type 0
 Dir.Dev 0
 Dir.Type 0
 Dir.Dev 0
 Dir.Qid.Type 0
-Dir.Qid.Version 2312512815
+Dir.Qid.Version 3320311213
 Dir.Qid.Path 11244905243916250064
-Dir.Mode 420
-Dir.Atime 2019-06-03T06:05:00+01:00
-Dir.Mtime 2019-06-03T06:05:00+01:00
-Dir.Length 1141
+Dir.Mode 438
+Dir.Atime 2019-06-23T23:42:58+01:00
+Dir.Mtime 2019-06-23T23:42:58+01:00
+Dir.Length 1190
 Dir.Name "go.mod"
 Dir.Uid "ng"
 Dir.Gid ""
 Dir.Gid ""
 Dir.Muid ""
 blocks:
-	edb3cd14097bf6629a2102380d04dcc2c45d013d7d608f9a3e705e7d0a47aa9c
+	c4756e75ad60dce960d82fbc56ed77ea96cb2ca2e864b4b61a8a8aa4eceee494
### Skipped 23 common lines ###
 
 replace github.com/lionkov/go9p => ../go9p
 
+replace github.com/sirupsen/logrus => ../logrus
+
 go 1.13
```

To allow for disconnected operation, each host running musclefs
corresponds to a separate tree. Such trees can be merged (think merging
git branches) in such a way that all these trees converge to the same
data. (This is the clunkiest part of the system but works for me.)

All blobs are encrypted before being sent to cloud storage. But a big
caveat, I'm not at all an expert and the encryption might be stupidly
weak.

In case you wonder, the project name is entirely random. It was supposed
to be a temporary name.

## Motivation

Ever since I read about Plan 9 and the possibility of having the same
experience on any machine participating in a Plan 9 cluster, I longed
for that experience in the operating systems I use (mainly Linux, with
plan9port, sometimes a 9front VM).

I believe that one should try to find already existing software
before embarking on writing something new.  At some point I found out
about the [Upspin](https://upspin.io) project and started using it -
this seemed to provide at least part of that experience and a lot
more interesting features. You probably want to use that software,
not this. Anyway. Upspin requires running a server somewhere on the
internet and be always connected and I didn't want to do that. I don't
want to maintain the server, and my lousy connectivity would make that
solution not work well for $HOME. So I tried to roll my own solution.

# Goals

The file server should run locally, I don't want to go across the
network for dir operations, but only to fetch data that's no longer
present locally, or to upload data that was locally created. The file
server should use the cloud for persistence and a local cache for fast
local usage.

The file server should work without an internet connection (provided the
data it needs is locally cached - in particular, entirely new data will
work locally).

The file server should preserve metadata changes such as permissions.

The file server should allow local modifications to a file, e.g.,
modifying a few bytes in a 600 MB file should not require to re-encode
and upload 600 MB worth of data.

The data model should allow automatic snapshots, fine grained, connected
in a history with generation of diffs between snapshots.

# Why 9P

The file server is implemented using the 9P protocol because it allows
mounting it on many operating systems in a number of ways. I sometimes
mount it with 9pfuse, but most often with v9fs in Linux. Sometimes I
mount it in Plan 9 of course.

# How it works

The filesystem tree is a Merkle tree. It is stored in a key-value store
where the key of a value is its cryptographic hash.  If the contents of
`root/dir/sub/thing` are modified, or its metadata changed, the key
corresponding to that filesystem node changes, because it is the hash
of the contents. This percolates up so that `root/dir/sub/` also changes
key, as one of its child nodes has changed, and so on and so forth until
we have a new key for the `root/`.

As a corollary, for every change to the filesystem tree the root key
changes. This data is maintained in memory and only periodically (e.g.,
once every couple of minutes) flushed to a staging area on disk, and
a new revision is created. A revision is a complete snapshot of the
filesystem tree and points to a parent revision.  This means that we
have a history of revisions. It can be inspected with `muscle history`.

When taking a snapshot via `echo snapshot > /n/muscle/ctl`, relevant data
is copied to the local cache (blocking the file server while doing so, but
this phase is fast), asynchronously uploaded to the persistent storage,
and remaining garbage is removed from the staging area. The garbage is due
to intermediate revisions that are not kept, for example, starting with

    s0=r0 < r1 < r2

where s0 is a snapshot and r0, r1, r2 are revisions, which are
automatically flushed every couple of minutes; when issuing a snapshot,
we'll get to

    s0=r0 < s2

where s2 corresponds to r2 and r1 is lost, garbage. That is, we keep all
the very fine-grained revisions but when we take a snapshot we discard the
intermediate revisions. This is a workaround aimed at reducing uploads
and because at some point I changed mind and thought that I didn't need
to persist a new revision every 2 minutes. Although that was quite useful
at times when deleting a file that I shouldn't have.

At the end of snapshot the staging area will be empty and local and
remote history will coincide. The only way to know if all data has been
propagated to persistent storage is by looking at the propagation log
file, and count the number of lines marked `todo` with the number of
lines marked `done`.

```
% cat lib/muscle/propagation.log
...
2019-06-23T12:00:02+01:00 todo 00a32d5f7dd910603a1b988ee0c3f6f9786e15889475e4b194efa25997fb21eb
2019-06-23T12:00:02+01:00 done 00a32d5f7dd910603a1b988ee0c3f6f9786e15889475e4b194efa25997fb21eb
...
```

The in-memory data can be flushed to disk also by issuing a flush command
with `echo flush >> /n/muscle/ctl`, otherwise its done automatically every
2 minutes. Data will also be flushed to disk when terminating `musclefs`
with SIGINT or SIGTERM. Don't SIGKILL unless you absolutely have to!

# Getting started

I'm surprised you want to try it, but thanks.

Install with `go get -u github.com/nicolagi/muscle/cmd/...`.

Look at `config/config.go` to understand what the JSON configuration
to be stored in `$HOME/lib/muscle/config` has to look like. This file
should have permissions 0600 because it contains the encryption key.

Start `musclefs` (it does not fork so send it to the background).

Mount in Linux with options similar to the below, or use `9pfuse`
from plan9port.

```
ng@clausius:/n/muscle$ mount | grep 9p
...
127.0.0.1 on /n/muscle type 9p (rw,relatime,sync,dirsync,dfltuid=1000,dfltgid=1004,access=1000,trans=tcp,noextend,port=4646)
...
```
