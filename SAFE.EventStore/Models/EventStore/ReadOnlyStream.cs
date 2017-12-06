using System;
using System.Collections.Generic;
using System.Linq;

namespace SAFE.EventStore.Models
{
    public class ReadOnlyStream
    {
        public string StreamName { get; private set; }
        public Guid StreamId { get; private set; }
        public List<EventData> Data { get; private set; }

        public ReadOnlyStream(string streamName, Guid streamId, List<EventBatch> batches)
        {
            StreamName = streamName;
            StreamId = streamId;

            Data = batches
                .SelectMany(x => x.Body)
                .OrderBy(x => x.MetaData.SequenceNumber)
                .ToList();

            if (Data.Count != Data.Select(x => x.MetaData.SequenceNumber).Distinct().Count())
                throw new ArgumentException("Duplicate sequence numbers!");

            if (Data.First().MetaData.SequenceNumber != 0)
                throw new ArgumentException("Incomplete stream!");

            // Use the isInSequnce test done 
            // in EventBatch ctor to try 
            // if they all are in sequence.
            new EventBatch($"{streamName}@{streamId}", StreamId, Data);
        }
    }
}
