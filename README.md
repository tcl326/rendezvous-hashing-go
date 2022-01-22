# rendezvous-hashing-go
Rendezvous Hashing in Golang

This library provides an implementation of Weighted Rendezvous Hashing.

For more details about Rendezvous Hashing and its weighted variants see:
- https://en.wikipedia.org/wiki/Rendezvous_hashing
- http://www.eecs.umich.edu/techreports/cse/96/CSE-TR-316-96.pdf
- https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf

The implementation is meant to be threadsafe.

## Credit
This library is heavily inspired by this already excellent [consistent hashing library](https://github.com/buraksezer/consistent). This library's interface is heavily derivative of the above mention library's. If you dive into the code you will see many similarities between the two libraries.

The implementation of the Rendezvous Hashing algorithm is the same as the one outlined here https://en.wikipedia.org/wiki/Rendezvous_hashing#Weighted_rendezvous_hash. 

## License
MIT License