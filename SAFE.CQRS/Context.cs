using SAFE.EventStore.Models;
using SAFE.SystemUtils;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.CQRS
{
    public class Context<TCmd, TAggregate> where TCmd : Cmd where TAggregate : Aggregate
    {
        readonly TCmd _cmd;
        readonly Repository _repo;
        TAggregate _instance;

        public Context(TCmd cmd, Repository repo)
        {
            _cmd = cmd;
            _repo = repo;
        }

        public async Task<bool> ExecuteAsync(Func<TCmd, TAggregate, Task<bool>> action)
        {
            if (_instance != null)
                throw new InvalidOperationException("Can only execute once!");
            _instance = await Locate(_cmd);
            return await action(_cmd, _instance);
        }

        public async Task<bool> CommitAsync()
        {
            if (_instance == null)
                throw new InvalidOperationException("Cannot commit before executing!");

            var events = _instance.GetUncommittedEvents();
            if (events.Count == 0)
                throw new InvalidOperationException("Already committed.");

            var data = events.Select(e => new EventData(
                e.AsBytes(),
                _cmd.CorrelationId,
                _cmd.Id,
                e.GetType().AssemblyQualifiedName,
                e.Id,
                e.GetType().Name,
                e.SequenceNumber,
                e.TimeStamp))
            .ToList();

            var batch = new EventBatch(_instance.StreamKey, _cmd.Id, data);

            if (!await _repo.Save(batch))
                return false;

            _instance.ClearUncommittedEvents();
            return true;
        }

        async Task<TAggregate> Locate(Cmd cmd)
        {
            var streamKey = StreamKeyFromCmd(cmd);
            return await _repo.GetAR<TAggregate>(streamKey, cmd.ExpectedVersion);
        }

        // The cmd will hold information
        // on which stream key it is intended for.
        // since every cmd map to exactly one aggregate 
        // type and the aggregate type (stream name) together 
        // with cmd property TargetId (stream id) will 
        // form the StreamKey. (StreamKey = [StreamName]@[StreamId])
        string StreamKeyFromCmd(Cmd cmd)
        {
            // todo: get all types deriving from Aggregate
            // get all their valid cmds, and build a map
            // the map could actually be stored on safenet
            switch(cmd.GetType().Name)
            {
                case "AddNote":
                    return $"NoteBook@{cmd.TargetId}";
                default:
                    return "SomeStreamKey";
            }
        }
    }
}