This directory contains two encryption keys - `ca.key` and `server.key`,
as well as a certificate showing that `ca` has signed `server`.

These are used for testing that we can install a trusted key and then connect
to servers identifying themselves with a certificate signed by that trusted
key.
