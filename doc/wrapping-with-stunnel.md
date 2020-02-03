# Using stunnel to safely expose musclefs on a public network

## Introduction and motivation

As explained in [the home page](https://github.com/nicolagi/muscle#goals),
musclefs is intended to run on each machine that needs access to its
data and it is meant to listen on localhost.

Therefore, having musclefs listen on a public network safely is not
a goal.

But should you want to do that, e.g., to let many clients connect
to the same musclefs instance instead of letting instances develop
independently and later merge them, you could use stunnel to wrap the
connection to musclefs, to address both the fact that musclefs

* uses plain TCP, and
* does not require authentication/authorization.

Stunnel can act as a frontend to musclefs, so that traffic will flow
unencrypted only between the stunnel process on the server host and the
musclefs on the same server host, on the loopback interface. Traffic
that traverses a public network will be encrypted instead.

A similar set-up will be done on the client side, to limit unencrypted
data flow to the loopback interface in the client.

In addition, using stunnel with pre-shared keys is a simple set up that
addresses both the transport layer security and the authentication and
authorization.

Note: To handle transport layer security one would probably use [tlssrv
and friends](http://man.cat-v.org/9front/8/tlssrv) on a Plan 9 system.

Another side node: While the 9P protocol that musclefs uses has [authentication
messages](https://9fans.github.io/plan9port/man/man9/attach.html),
musclefs itself does not support authentication. To do so,
one would have to add proper support to musclefs and also
set up a factotum as well as a secstore. See [Security in Plan
9](http://doc.cat-v.org/plan_9/4th_edition/papers/auth).

## Step 1: Configure stunnel in the server host

You can copy and customize the snippet below and place it in
`$HOME/lib/muscle/stunnel.config`, where all other muscle-related
files live.

```
pid = /home/user/lib/muscle/stunnel.pid
output = /home/user/lib/muscle/stunnel.log

[muscle]
accept = 3004
connect = 2323
ciphers = PSK
PSKsecrets = /home/user/lib/muscle/stunnel.keys
```

The `accept` line refers to the port on which stunnel listens. Traffic to/from
this will be encrypted.

The `connect` line refers to the port to which stunnel connects, and must be
configured to the metadataserver listening port.

The `ciphers` and `PSKsecrets` lines tell stunnel to use a pre-shared key (one
per client). This seems to be the simplest set-up to start with, but you can
configure stunnel to use certificates instead.

The file `stunnel.keys` associates client names with passwords. Its content
could be as follows.

```
client-host:shiPaetai3ejaiGees3koi3nuoph5ohm
```

Ensure permissions are locked down to your user with

```
chmod 0600 stunnel.keys
```

Now launch stunnel with

```
stunnel stunnel.config
```

You can verify stunnel is alive and listening with commands like:

```
netstat -ltn | grep 3004
lsof `pidof stunnel` | grep TCP
```

If you can't verify stunnel is running, you'll have to look at the logs in
`$HOME/lib/muscle` for possible problems.

## Step 2: Configure stunnel on the client host

The configuration is similar on a client host. Configuration similar to the one
below should be placed at `$HOME/lib/muscle/stunnel.config`.

```
pid = /home/user/lib/muscle/stunnel.pid
output = /home/user/lib/muscle/stunnel.log

[muscle]
client = yes
accept = localhost:2323
connect = server-host:3004
PSKidentity = client-host
PSKsecrets = /home/user/lib/muscle/stunnel.keys
```

The `client` line declares this stunnel should run in client mode, i.e.,
connect to another stunnel process as indicated by the `connect` line.

The `accept` line specifies that this stunnel listens on port 2323 on the
loopback interface. Thus localhost:2323 is the address that should be mounted.

The `PSKsecrets` file is in the same format as for the server. It only needs to
include the line corresponding to the identity specified as `PSKidentity`, in
this case, client-host. The server instead, must contains one line per client
(or to be more precise, per identity).

Start and validate stunnel as done for the server.

## Step 3: Mount via the local stunnel port

Mount with the usual command, e.g.,

```
sudo umount /n/muscle
sudo mount 127.0.0.1 /n/muscle -t 9p -o 'trans=tcp,port=2323,dfltuid=1000,dfltgid=1000,uname=ng,access=any'
```

only now localhost:2323 will be stunnel, not musclefs.

## Appendix: Inspecting traffic with tcpdump

Commands such as those below will help validate that traffic that go via the
public network is encrypted while the local traffic is unencrypted. These
should be run in both client and server hosts while the file system is in use.

```
sudo tcpdump -A port 2323
sudo tcpdump -A port 3004
```

## Appendix: Keeping processes alive

For a very simple approach to the problem of keeping the processes alive, a
tool like [mon](https://github.com/tj/mon) could be useful.  Mon handles
transient failures well and allows to quickly restart a process, e.g., to pick
up a new binary, by simply killing the running instance.
