using System;

namespace SAFE.SystemUtils.Events
{
    /// <summary>
    /// OBSERVE: Properties cannot be of type IEnumerable
    /// they will not be deserialized if they are!
    /// </summary>
    public abstract class Event
    {
        public Guid Id { get; private set; }

        public DateTime TimeStamp { get; private set; }

        public Event()
        {
            Id = SequentialGuid.NewGuid();
            TimeStamp = SystemTime.UtcNow;
        }

        protected void SetId(Guid id)
        {
            Id = id;
        }

        public int SequenceNumber { get; set; }
    }

    // TODO: move to implementing library
    public abstract class AccountingEvent : Event
    {
        public AccountingEvent(DateTime accountingDate)
        {
            AccountingDate = accountingDate;
        }
        public DateTime AccountingDate { get; private set; }
    }
}