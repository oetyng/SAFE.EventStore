using SAFE.SystemUtils;
using System;

namespace SAFE.CQRS
{
    public abstract class Cmd
    {
        public Guid Id { get; private set; }
        public Guid TargetId { get; private set; }
        public Guid CorrelationId { get; internal set; }

        public Cmd(Guid targetId)
        {
            TargetId = targetId;
            Id = SequentialGuid.NewGuid();
        }
    }
}