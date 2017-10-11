using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SAFE.EventStore.Models
{
    public class StreamKey
    {
        public StreamKey(string streamName, Guid streamId)
        {
            StreamName = streamName;
            StreamId = streamId;
        }

        public StreamKey(string streamKey)
        {
            (StreamName, StreamId) = GetKeyParts(streamKey);
        }

        public string StreamName { get; private set; }
        public Guid StreamId { get; private set; }
        public string Value { get { return $"{StreamName}@{StreamId}"; } }

        public static implicit operator string(StreamKey streamKey)
        {
            return streamKey.Value;
        }

        public static implicit operator (string, Guid) (StreamKey streamKey)
        {
            return (streamKey.StreamName, streamKey.StreamId);
        }

        (string, Guid) GetKeyParts(string streamKey)
        {
            var source = streamKey.Split('@');
            var streamName = source[0];
            var streamId = new Guid(source[1]);
            return (streamName, streamId);
        }
    }

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
                .OrderBy(x => x.SequenceNumber)
                .ToList();

            if (Data.Count != Data.Select(x => x.SequenceNumber).Distinct().Count())
                throw new ArgumentException("Duplicate sequence numbers!");

            if (Data.First().SequenceNumber != 0)
                throw new ArgumentException("Incomplete stream!");

            // Use the isInSequnce test done 
            // in EventBatch ctor to try 
            // if they all are in sequence.
            new EventBatch($"{streamName}@{streamId}", StreamId, Data);
        }
    }

    public class EventBatch : IComparable, IEquatable<EventBatch>
    {
        [JsonProperty("streamKey")]
        public string StreamKey { get; private set; }

        [JsonProperty("causationId")]
        public Guid CausationId { get; private set; }
        
        [JsonProperty("timeStamp")]
        public DateTime TimeStamp { get; private set; }
        
        [JsonProperty("body")]
        public List<EventData> Body { get; private set; }

        [JsonIgnore]
        public string LocalTime => Convert.ToDateTime(TimeStamp).ToString("f");

        public EventBatch(string streamKey, Guid causationId, List<EventData> events)
        {
            if (!IsInSequence(events) || events.Count == 0)
                throw new ArgumentOutOfRangeException(nameof(events));

            Body = events;
            StreamKey = streamKey;
            CausationId = causationId;
            TimeStamp = events.Last().TimeStamp;
        }

        bool IsInSequence(List<EventData> events)
        {
            var firstEvent = events.First();
            int[] streamSequence = new int[events.Count];
            var first = firstEvent.SequenceNumber;
            for (int i = 0; i < events.Count; i++)
                streamSequence[i] = events[i].SequenceNumber - first;

            bool isInSequence = streamSequence.SequenceEqual(Enumerable.Range(0, streamSequence.Count())); // useful to have as extension method list.InSequenceBy<int>(x => x.SomeIntProperty); and list.InSequenceByDescending<int>(x => x.SomeIntProperty);

            return isInSequence;
        }

        #region Equality
        public int CompareTo(object obj)
        {
            var other = obj as EventBatch;
            if (other == null)
                throw new NotSupportedException();

            var thisDt = Convert.ToDateTime(TimeStamp);
            var otherDt = Convert.ToDateTime(other.TimeStamp);
            return thisDt.CompareTo(otherDt);
        }

        public bool Equals(EventBatch other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return Guid.Equals(CausationId, other.CausationId) &&
                DateTime.Equals(TimeStamp, other.TimeStamp) && Enumerable.SequenceEqual(Body, other.Body);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((EventBatch)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = CausationId != null ? CausationId.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ (TimeStamp != null ? TimeStamp.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Body != null ? Body.GetHashCode() : 0);
                return hashCode;
            }
        }
        #endregion Equality
    }
}
