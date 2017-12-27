using SAFE.CQRS;
using System;
using System.Threading.Tasks;
using Context = SAFE.CQRS.Context<SAFE.TestCQRSApp.AddNote, SAFE.TestCQRSApp.NoteBook>;

namespace SAFE.TestCQRSApp
{
    public class NoteBookCmdHandler
    {
        Repository _repo;

        public NoteBookCmdHandler(Repository repo)
        {
            _repo = repo;
        }

        public async Task<bool> Handle(AddNote cmd)
        {
            try
            {
                var ctx = new Context(cmd, _repo);

                var changed = await ctx.ExecuteAsync((c, ar) =>
                {
                    if (ar.Version == -1)
                        ar.Init(cmd.TargetId).GetAwaiter().GetResult();
                    return ar.AddNote(c.Note);
                });

                if (!changed)
                    return false;

                var savedChanges = await ctx.CommitAsync();

                return savedChanges;
            }
            catch (InvalidOperationException ex)
            {
                // logging
                throw;
            }
            catch (Exception ex)
            {
                // logging
                throw;
            }
        }
    }

    // A cmd type can only ever have one recipient type. (ExampleCmd is only handled by ExampleAggregate).
    public class AddNote : Cmd
    {
        public AddNote(Guid targetId, int expectedVersion, string note)
            : base(targetId, expectedVersion)
        {
            Note = note;
        }

        public string Note { get; private set; }
    }
}