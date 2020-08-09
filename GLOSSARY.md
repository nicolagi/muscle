<!--
Edit /^$/+,$|sort
Trying to help myself more than anyone, here...
-->

graft: the operation of attaching a subtree, e.g., root/src/muscle, from a donor revision, into, say, root/my/other/src/muscle, in a receiver revision. Grafting is an operation used as part of merging.
hash pointer: a cryptographic hash (currently SHA2) of a blob. When storing blobs in a key value store, the hash pointer is the key and the blob is the value (content-addressing).
paired store: pairs the cache with the remote store, adding asynchronous propagation of items.
persistent store: same as remote store.
remote store: a key value store to use for long-term persistence, e.g., built on top of S3.
revision: an object that holds a root pointer and a pointers to a parent revision. A revision contains some more metadata, like host name and time at which the revision was created. This notion is similar to that of a commit in git.
root pointer: a hash pointer to the root node metadata of a file system tree.
staging area: a key value store representing data that hasn't been consolidated into a revision and propagated to persistent storage. Current implementation is disk-based.
