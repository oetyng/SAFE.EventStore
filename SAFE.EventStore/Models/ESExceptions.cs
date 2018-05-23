using System;

namespace SAFE.EventStore
{
    public class DatabaseNotFoundException : Exception
    {
        public DatabaseNotFoundException(string msg)
              : base(msg)
        { }
    }

    public class CategoryNotFoundException : Exception
    {
        public CategoryNotFoundException(string msg)
            : base(msg)
        { }
    }

    public class StreamKeyNotFoundException : Exception
    {
        public StreamKeyNotFoundException(string msg)
            : base(msg)
        { }
    }

    public class StreamKeyPartitionNotFoundException : Exception
    {
        public StreamKeyPartitionNotFoundException(string msg)
            : base(msg)
        { }
    }

    public class DataMdNotFoundException : Exception
    { }

    public class PartitionMdNotFoundException : Exception
    { }
}
