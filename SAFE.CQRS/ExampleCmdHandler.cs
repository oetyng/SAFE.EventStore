using SAFE.SystemUtils.Events;
using System;
using System.Threading.Tasks;
using Context = SAFE.CQRS.Context<SAFE.CQRS.ExampleCmd, SAFE.CQRS.ExampleAggregate>;

namespace SAFE.CQRS
{
    public class ExampleCmdHandler
    {
        Repository _repo;

        public ExampleCmdHandler(Repository repo)
        {
            _repo = repo;
        }

        public async Task<bool> Handle(ExampleCmd cmd)
        {
            try
            {
                var ctx = new Context(cmd, _repo);

                var changed = await ctx.ExecuteAsync((c, ar) =>
                    ar.DoSomething(c.ExampleProperty));

                if (!changed)
                    return false;

                var savedChanges = await ctx.CommitAsync();

                return savedChanges;
            }
            catch (InvalidOperationException ex)
            {
                // logging
                return false;
            }
            catch (Exception ex)
            {
                // logging
                return false;
            }
        }
    }

    public class ExampleAggregate : Aggregate
    {
        int _exampleProperty;

        public async Task<bool> DoSomething(int exampleProperty)
        {
            await Task.FromResult(0);

            var validOperation = exampleProperty > 0;

            if (!validOperation) // protect invariants
                throw new InvalidOperationException("exampleProperty must be > 0");

            if (exampleProperty == _exampleProperty)
                return false; // i.e. nothing changed

            RaiseEvent(new ExampleEvent(this.StreamId, exampleProperty)); // applies the change to current state

            return validOperation; // returning true means state changed
        }

        void Apply(ExampleEvent e)
        {
            _exampleProperty = e.ExampleProperty;
        }
    }

    // A cmd type can only ever have one recipient type. (ExampleCmd is only handled by ExampleAggregate).
    public class ExampleCmd : Cmd
    {
        public ExampleCmd(Guid targetId, int expectedVersion, int exampleProperty)
            : base(targetId, expectedVersion)
        {
            ExampleProperty = exampleProperty;
        }

        public int ExampleProperty { get; private set; }
    }

    public class ExampleEvent : Event
    {
        public ExampleEvent(Guid exampleStreamId, int exampleProperty)
        {
            ExampleStreamId = exampleStreamId;
            ExampleProperty = exampleProperty;
        }

        public Guid ExampleStreamId { get; private set; }
        public int ExampleProperty { get; private set; }
    }
}