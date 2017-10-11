using SAFE.EventStore.Models;
using SAFE.EventStore.Services;
using SAFE.SystemUtils;
using SAFE.SystemUtils.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.EventStore.UI.Setup
{
    class MockEventStoreService : IEventStoreService
    {
        Dictionary<string, Dictionary<string, Dictionary<string, ReadOnlyStream>>> _allDbs = new Dictionary<string, Dictionary<string, Dictionary<string, ReadOnlyStream>>>();

        public MockEventStoreService()
        {
            var databases = new List<string> { "SmallDb", "IOT", "Weather", "Trading" };

            databases.ForEach(db =>
            {
                switch (db)
                {
                    case "SmallDb":
                        LoadEntries(db, new[] { "Food", "Milk", "Dog", "Cat", });
                        break;
                    case "IOT":
                        LoadEntries(db, new[] { "Car", "Fridge", "Climate", "Alarm", });
                        break;
                    case "Weather":
                        LoadEntries(db, new[] { "Wind", "Rain", "Clouds", "Sun", });
                        break;
                    case "Trading":
                        LoadEntries(db, new[] { "Order", "Trade", "Wallet", "CurrencyPair", });
                        break;
                }
            });
        }

        Random _rand = new Random(new Random().Next());

        void LoadEntries(string db, IEnumerable<string> categories)
        {
            _allDbs[db] = new Dictionary<string, Dictionary<string, ReadOnlyStream>>();

            categories
               .Select(category =>
                    _allDbs[db][category] = new Dictionary<string, ReadOnlyStream>())
                .ToList();

            var streamKeys =
                categories.SelectMany(category => 
                    Enumerable.Range(0, _rand.Next(8, 16))
                    .Select(_ => $"{category}@{Guid.NewGuid()}"))
                .ToList();

            streamKeys
                .Select(streamKey =>
                    StoreBatchAsync(db, new EventBatch(streamKey, Guid.NewGuid(), GetEventDataList(streamKey))).GetAwaiter().GetResult())
                .ToList();
        }

        public Task CreateDbAsync(string databaseId)
        {
            var couldAdd = _allDbs.TryAdd(databaseId, new Dictionary<string, Dictionary<string, ReadOnlyStream>>());
            return Task.FromResult(couldAdd);
        }

        public void Dispose()
        {

        }

        public Task<List<DatabaseId>> GetDatabaseIdsAsync()
        {
            return Task.FromResult(_allDbs.Keys.Select(x => new DatabaseId(x)).ToList());
        }

        public Task<Result<ReadOnlyStream>> GetStreamAsync(string databaseId, string streamKey)
        {
            try
            {
                var (streamName, _) = GetKeyParts(streamKey);

                var stream = _allDbs[databaseId][streamName][streamKey];

                return Task.FromResult(Result.OK(stream));
            }
            catch(Exception ex)
            {
                return Task.FromResult(Result.Fail<ReadOnlyStream>(ex.Message));
            }
        }

        public Task<List<string>> GetStreamKeysAsync(string databaseId, string streamType)
        {
            var keys = _allDbs[databaseId][streamType]
                .Select(c => GetStreamKey(c.Value))
                .ToList();

            return Task.FromResult(keys);
        }

        public Task<List<string>> GetCategoriesAsync(string databaseId)
        {
            var categories = _allDbs[databaseId]
                .Select(db => db.Key)
                .ToList();

            return Task.FromResult(categories);
        }

        public Task<Result<bool>> StoreBatchAsync(string databaseId, EventBatch batch)
        {
            if (!_allDbs.ContainsKey(databaseId))
                return Task.FromResult(Result.Fail<bool>($"Database id {databaseId} does not exist!"));
            
            var db = _allDbs[databaseId];
            var (category, streamId) = GetKeyParts(batch.StreamKey);

            var expectedVersion = batch.Body.First().SequenceNumber - 1;

            var containsCategory = db.ContainsKey(category);
            var containsKey = containsCategory && db[category].ContainsKey(batch.StreamKey);
            if (expectedVersion > -1 && !containsKey)
                return Task.FromResult(Result.Fail<bool>($"The stream does not exist!"));
            if (expectedVersion < 0 && containsKey)
                return Task.FromResult(Result.Fail<bool>($"The stream already exists!"));

            if (expectedVersion == -1)
            {
                if (!containsCategory)
                    db[category] = new Dictionary<string, ReadOnlyStream>();
                _allDbs[databaseId][category][batch.StreamKey] = new ReadOnlyStream(category, streamId, new[] { batch }.ToList());
                return Task.FromResult(Result.OK(true));
            }

            var stream = _allDbs[databaseId][category][batch.StreamKey];
            var version = stream.Data.First().SequenceNumber;
            if (version != expectedVersion)
                return Task.FromResult(Result.Fail<bool>($"Concurrency exception! Expected {expectedVersion} but found {version}."));

            stream.Data.AddRange(batch.Body);
            
            return Task.FromResult(Result.OK(true));
        }

        (string, Guid) GetKeyParts(string streamKey)
        {
            var source = streamKey.Split('@');
            return (source[0], new Guid(source[1]));
        }

        string GetStreamKey(ReadOnlyStream stream)
        {
            return $"{stream.StreamName}@{stream.StreamId}";
        }

        List<EventData> GetEventDataList(string streamKey)
        {
            var baseLine = DateTime.UtcNow;
            var eventRange = Enumerable.Range(0, _rand.Next(8, 16)).ToList();
            var count = eventRange.Count;
            return eventRange
                .Select(nr =>
                {
                    var e = new MockEvent(GetKeyParts(streamKey).Item1, _rand.Next(99999));
                    e.SequenceNumber = nr;
                    return new EventData(
                         e.AsBytes(),
                         Guid.NewGuid(),
                         Guid.NewGuid(),
                         typeof(MockEvent).AssemblyQualifiedName,
                         Guid.NewGuid(),
                         typeof(MockEvent).Name,
                         nr,
                         GetTimeStamp(baseLine, count--));
                }).ToList();
        }

        DateTime GetTimeStamp(DateTime baseLine, int count)
        {
            return baseLine
                .AddHours(-count)
                .AddMinutes(-(_rand.Next(0, ++count)))
                .AddSeconds(-(_rand.Next(0, --count)))
                .AddMilliseconds(-(_rand.Next(0, ++count)));
        }

        class MockEvent : Event
        {
            public MockEvent(string stringProp, int intProp)
            {
                StringProp = stringProp;
                IntProp = intProp;
            }
            public string StringProp { get; private set; }
            public int IntProp { get; private set; }
        }
    }
}