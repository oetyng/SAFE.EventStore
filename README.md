# SAFE.EventStore
EventSourcing database on SAFE Network

Based on examples in MaidSafe's mobile repo (https://github.com/maidsafe/safe_mobile).

Configured for alpha-2 network.

To start using event sourcing as a database solution - for parts of, or entire applications - reference the SAFE.EventStore library in 
your .NET project, and have a complete framework for storing and retrieving data as events in streams.
To have a graphical UI for the DB management, access this from the webapp running on localhost.

Can be used both with a mock eventstore, and with the real SAFENetwork storage.
This can be changed in EventStore.UI project, Startup.cs class. 

Storing in the actual alpha-2 SAFE network can be tried with the NoteBook example.
Requires SAFE Browser v0.6.0 to to authorize the app: https://github.com/maidsafe/safe_browser/releases/tag/alpha-2
