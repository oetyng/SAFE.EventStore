using Newtonsoft.Json;
using System;

namespace SAFE.EventStore.Models
{
    public class MetaData
    {
        [JsonConstructor]

        MetaData()
        { }

        public MetaData(
            Guid correlationId,
            Guid causationId,
            string eventClrType,
            Guid id,
            string name,
            int sequenceNumber, DateTime timeStamp)
        {
            CorrelationId = correlationId;
            CausationId = causationId;
            EventClrType = eventClrType;
            Id = id;
            Name = name;
            SequenceNumber = sequenceNumber;
            TimeStamp = timeStamp;
        }

        [JsonProperty("eventClrType")]
        public string EventClrType { get; private set; }

        [JsonProperty("id")]
        public Guid Id { get; private set; }

        [JsonProperty("sequenceNumber")]
        public int SequenceNumber { get; private set; }

        [JsonProperty("name")]
        public string Name { get; private set; }

        [JsonProperty("timeStamp")]
        public DateTime TimeStamp { get; private set; }

        [JsonProperty("correlationId")]
        public Guid CorrelationId { get; private set; }

        [JsonProperty("causationId")]
        public Guid CausationId { get; private set; }
    }
}
