using Newtonsoft.Json;
using System;

namespace SAFE.EventStore.Models
{
    public class EventRelayMsg
    {
        EventData _deserialized;

        [JsonConstructor]
        EventRelayMsg() { }

        public EventRelayMsg(byte[] payload,
            Guid id,
            string name,
            int sequenceNumber,
            string streamName, Guid streamId, DateTime timeStamp)
        {
            Payload = payload;
            Id = id;
            Name = name;
            SequenceNumber = sequenceNumber;
            StreamName = streamName;
            StreamId = streamId;
            TimeStamp = timeStamp;
        }

        public EventData GetDeserialized(Func<byte[], string, EventData> deserialize)
        {
            if (_deserialized == null)
                _deserialized = deserialize(Payload, typeof(EventData).AssemblyQualifiedName);
            return _deserialized;
        }

        public byte[] Payload { get; private set; }

        public Guid Id { get; private set; }

        public int SequenceNumber { get; private set; }

        public string Name { get; private set; }

        public string StreamName { get; private set; }

        public Guid StreamId { get; private set; }

        public string StreamKey { get { return $"{StreamName}@{StreamId}"; } }

        public DateTime TimeStamp { get; private set; }
    }
}