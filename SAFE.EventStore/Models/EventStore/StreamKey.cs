using System;

namespace SAFE.CQRS.Stream
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
}