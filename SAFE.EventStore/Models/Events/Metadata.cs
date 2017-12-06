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

        public string EventClrType { get; private set; }

        public Guid Id { get; private set; }

        public int SequenceNumber { get; private set; }

        public string Name { get; private set; }

        public DateTime TimeStamp { get; private set; }

        public Guid CorrelationId { get; private set; }

        public Guid CausationId { get; private set; }
    }
}
