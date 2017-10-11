# SAFE.EventStore
EventSourcing database on SAFE Network

Based on examples in MaidSafe's mobile repo (https://github.com/maidsafe/safe_mobile).

Configured for alpha-2 network.

To start using event sourcing as a database solution - for parts of, or entire applications - reference the SAFE.EventStore library in 
your .NET project, and have a complete framework for storing and retrieving data as events in streams.
To have a graphical UI for the DB management, access this from the webapp running on localhost.

Currently hardcoded to use a mock eventstore.
This can be changed in EventStore.UI project, Startup.cs class. 
But currently storing in the actual alpha-2 SAFE network, will not work as the connection with native libs is not yet completed.
