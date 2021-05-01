**Update 2021-05-01.**

The fs structure has changed.
The current contents are now in `/live`, while past revisions are at `/$sha` (such revision nodes are attached to the tree by walking to them).
The program `cmd/snapshotsfs` is gone.
The control file remains at `/ctl` (thus one can now create `/live/ctl` as a regular file or dir if required).
In other words:

	; ls /m
	945e153f1c6ce898e47e06df1ed2998d7649b18d37601d49e047ff3801130fa3
	9639fc0640c014900c4525ccacc8837e50ce33cb351b4ed8e1e15b8e4fa8e58b
	ctl
	f0eed4956ba54f59520ae5f2dbd8c48b156ba2ff39661663139f8b3fc8e3a3ca
	live
	; muscle history | awk '/^key/{print $2}'
	a9249e30222ffbc7003c267defe508202a382372e2c93ef5652ebb2248ecf2f6
	e444c7389b47863da04bd45a3468169ee4f8211bc33f234ff22bd393cebf15b1
	9639fc0640c014900c4525ccacc8837e50ce33cb351b4ed8e1e15b8e4fa8e58b
	; ls -ld /m/a9249e30222ffbc7003c267defe508202a382372e2c93ef5652ebb2248ecf2f6
	drwx------ 1 nicolagi users 0 Apr 30 07:05 /m/a9249e30222ffbc7003c267defe508202a382372e2c93ef5652ebb2248ecf2f6
	; ls /m
	945e153f1c6ce898e47e06df1ed2998d7649b18d37601d49e047ff3801130fa3
	9639fc0640c014900c4525ccacc8837e50ce33cb351b4ed8e1e15b8e4fa8e58b
	a9249e30222ffbc7003c267defe508202a382372e2c93ef5652ebb2248ecf2f6
	ctl
	f0eed4956ba54f59520ae5f2dbd8c48b156ba2ff39661663139f8b3fc8e3a3ca
	live

**Update 2021-04-18.**

Lots of small bug fixes where done thanks for a [fs differential testing project](../fsdiff).

Musclefs supports tagging sub-sequences of revisions; these could be useful for tracking projects histories as sub-sequences of the whole fs history.
The commands for diff and history all support a new `-b` option (tag to use as base to diff from, show history of revisions with given tag only).
The push command, used to create new revisions, supports an optional list of additional tags (in addition to the default, "base") to add to the revision.

**Update 2020-10-11.**
This file system uses the 9P protocol.
It doesn't mean it's a Plan 9 file system like fossil or cwfs.
Not yet at least.
Read on for use cases.

**Update 2020-04-13.**
I haven't made functional changes in ages and I don't plan to; the programs here work very well for me.
I'm experimenting a lot with the code anyway and may introduce regressions as I experiment and try to simplify.
In retrospect, I should've have tried to extend a CWFS+Venti set up to do what I need, rather than rewrite something similar.
But of course, when I started this, I didn't know much about CWFS nor the Venti archival system...

# muscle

The muscle project consists of musclefs, snapshotsfs, and muscle.

The former two, musclefs and snapshotsfs, are 9P file servers, Amazon
S3-backed, with a git-like backend allowing for file system history
and file system merge. Musclefs serves the current revision of the
filesystem, while snapshotsfs serves any and all past revisions.
Revisions are linked together in a git-like history.

The latter, muscle, is a command-line tool that offers additional
operations on the same data that is exposed via the file server.

The rest of this page goes into technical matters instead.

## Overview

The storage backend of the file system consists of many layers, but is
ultimately backed by cloud storage. (That would be S3 at the time of
writing, but more can be added.) This makes it possible to expose and
operate on the same data in all computers. In particular, it makes it
possible to have that same data in a freshly installed machine, downloading
data from the cloud as necessary.

The file system can often be used without a persistent internet
connection, as data is stored locally as well. A local store is paired
with the remote store and acts as a write-back cache. There is
no automatic expiration of items from the cache, but that can be
achieved with `find` and, say, `-atime +30`, for blobs last
accessed more than a month ago. (I won't provide exact commands as what
needs to be escaped depends on the shell.)

The file system supports taking incremental snapshots, called revisions,
which are linked, in such a way that you can have a history of snapshots
akin to a git history of commits. In particular, it is possible to see
the diff between any two revisions of the file system.  For example,
showing names of modified files in recent revisions can be done as follows:

```
% muscle history -d -N | 9 grep '^$|^(key|root/)' | uniq
key 86420c0f76b8c4070e166dba6f8356adb80e7c51a3df23ee6b5871fffc041f01

root/src/muscle.wiki/Walkthrough.md

key c763eae2ef3441db7575649952cdf361e4ba83156ed3dd8dd0e6e61e59d68da2

root/src/muscle/README.md
root/src/muscle.wiki/Walkthrough.md
root/worklogs/bookmarks

key c603dd7f5d332fad15d4f5588caa1c62af8525f847d9db37629b668709820951

root/src/muscle.wiki/images
root/tmp/snippets-walk-through
```

To allow for disconnected operation, each host running musclefs
corresponds to a partial working copy of the whole fs.

To synchronize working copies, there are two commands, pull
(corresponding to git pull --rebase) and push (corresponding to git
push). The latter is only allowed after pull (corresponding to
fast-forward git merges). Other analogy: cvs update for pull, cvs
commit for push.

All blobs are encrypted before being sent to cloud storage. But a big
caveat, I'm not at all an expert and the encryption might be stupidly
weak.

In case you wonder, the project name is entirely random. It was supposed
to be a temporary name.

## Motivation

Ever since I read about Plan 9 and the possibility of having the same
data on any machine participating in a Plan 9 cluster, I longed
for that experience in the operating systems I use (mainly Linux, with
plan9port, sometimes a 9front VM these days).

I believe that one should try to find already existing software
before embarking on writing something new.  At some point I found out
about the [Upspin](https://upspin.io) project and started using it.
It seemed to provide at least part of that experience and a lot
of additional interesting features. You probably want to use that software,
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
mount it in Plan 9.

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

When taking a snapshot via `echo push > /n/muscle/ctl`, relevant data
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
file, and match the lines marked `todo` with those marked `done`.

```
% cat lib/muscle/propagation.log
...
2019-06-23T12:00:02+01:00 todo 00a32d5f7dd910603a1b988ee0c3f6f9786e15889475e4b194efa25997fb21eb
2019-06-23T12:00:02+01:00 done 00a32d5f7dd910603a1b988ee0c3f6f9786e15889475e4b194efa25997fb21eb
...
```

The in-memory data can be flushed to disk also by issuing a flush command
with `echo flush >/n/muscle/ctl`, otherwise its done automatically every
2 minutes. Data will also be flushed to disk when terminating `musclefs`
with SIGINT or SIGTERM. Don't SIGKILL unless you absolutely have to!

# Getting started

Install with `go get -u github.com/nicolagi/muscle/cmd/...`.

Get an initial configuration with `muscle init` and customize.
The [walk-through page](doc/walk-through.md) shows how.

Start `musclefs` and `snapshotsfs` as background processes.

Example mount commands:

	sudo mount 127.0.0.1 /mnt/muscle -t 9p -o 'trans=tcp,port=2323,dfltuid=1000,dfltgid=1000,uname=youruser'
	sudo mount `{namespace}^/muscle /mnt/muscle -t 9p -o 'trans=unix,dfltuid=1000,dfltgid=1000,uname=youruser'
	9pfuse 127.0.0.1:2323 /mnt/muscle
	9pfuse `{namespace}^/muscle /mnt/muscle

and similar for snapshotsfs.
