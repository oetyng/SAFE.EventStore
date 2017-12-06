using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SAFE.EventStore.Models
{
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
            TimeStamp = events.Last().MetaData.TimeStamp;
        }

        bool IsInSequence(List<EventData> events)
        {
            var firstEvent = events.First();
            int[] streamSequence = new int[events.Count];
            var first = firstEvent.MetaData.SequenceNumber;
            for (int i = 0; i < events.Count; i++)
                streamSequence[i] = events[i].MetaData.SequenceNumber - first;

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
