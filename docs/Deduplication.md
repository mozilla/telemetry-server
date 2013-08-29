On the 'idle-daily' problem and De-duplication of submissions
=============================================================

Firefox collects and submits data as follows.

Each time the browser starts up, a new Telemetry ID is generated. Telemetry
data is collected about the current session and is associated with that ID.
Upon shutdown, the session data is written to disk using that session's ID. 

Once a day, the collected sessions' data is submitted as "saved-session"
information, and the currently running session's data is submitted as
"idle-daily" information.

If we are only interested in completed sessions, this is no problem, since we
will see each saved-session submission once.

If we want to look at every submission, we run into a situation where the same
ID may have been submitted multiple times. A session's data may be submitted
one or more times as "idle-daily" (multiple submissions being possible for
sessions that last more than one day), and may also be submitted once more as
a "saved-session" after that session has completed.

We need some way of de-duplicating these submissions.

De-duplication
--------------

The HBase backend attempts to de-duplicate by using the ID as part of the key
used for data storage. Unfortunately, the key also includes the submission
date, so long-running sessions may still be counted more than once. A better
approach is required.

### Key/value storage
One obvious approach is to use the session ID as the key in a key/value store,
such that any later submission will replace the old submission's data with the
updated data.

This works fine in terms of deduplication, but does not scale well to the large
volumes of data that need to be handled (as of 2013-07-29, approximately
1.2TB/day of raw data).

### Tombstones
From Taras:

For map reduce jobs:
So every time you open(O_APPEND), write(telemetry) you record
idle_daily_uuid + position in the file. So then write those out
alongside the data file...Then the map/reduce job tracker has to read
those in and tell mappers which (filename, pos)* pairs to ignore.

Then during map reduce, some preliminary  job would scan all of the
idle-daily deduplication logs to pass above info to mappers.

So it's easy to deduplicate a single map/reduce run...To make this work
for dashboard we probably want to switch from dump logs to postgres so
we can calculate such things better(eg intersect millions of UUIDs to
find duplicates)

* where pos is position in file after readline()

Idle-daily deduplication notes:
* 86400(seconds in day)*250(submissions per second)*8(bytes per UUID)= 161mb -> 2gb for 12 weeks
* leveldb seems well-suited for this sort of workload, but a C++ implementation is trivial: https://github.com/tarasglek/tombstone_maker
* skiplists(filename:offset)  are called tombstones
* should have a compressed TOMBSTONE_INDEX for every IDLE_DAILY in a particular release. incoming_data EC2 job should generate those. Since these are basically sets they can be generated in parallel and UNIONED at the end of each EC2 job.

### Partial Key/Value storage
A variation on the plain key/value storage described above.

We only really need to keep the idle-daily submissions in a key/value store,
since it can safely be assumed that saved-sessions will only contain duplicates
in case of a client bug.

So we can keep idle-daily submissions in a key/value store, and with every
incoming submission we either replace (in the case of idle-daily submissions)
or delete (in the case of saved-session submissions) any existing submission
for that key.

If we also add an expiration policy where idle-daily submissions older than N
days are deleted, we should reach a steady state for idle-daily that includes
only the current, unique values plus any "abandoned" records in the past N
days.

Abandoned records would include such things as submission errors (network
problems, crashes, etc) on the client, new Firefox profiles, OS reinstalls,
etc. Anything where an idle-daily does not end up getting replaced with the
final saved-session.
