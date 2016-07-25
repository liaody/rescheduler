0.5.2
-----
* Fix logging with bad unicode
* Fix worker_index problem

0.5.1
-----
* worker_index for lockless synchronization
* Explicit locking through Rescheduler::Sync

0.5.0
-----
* Rescheduler::Worker support routines and binary
* New independent rk_args key setup to avoid contentions.
* Service routine "weak" mutual exclusion to avoid contentions.
* Lots of improvements for heavy queue operations.
* Contention free stats

0.4.0
-----
* MultiJson dependency (instead of JSON)
