using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using SAFE.DotNET.Models;
using SAFE.EventStore.Models;
using SAFE.SystemUtils;
using System.Collections.Concurrent;
using SAFE.EventSourcing.Models;
using SAFE.EventSourcing;
using SafeApp.Utilities;
using SafeApp;
using SafeApp.MData;
using SafeApp.Misc;
using SafeApp.IData;
using Partitioner = SAFE.EventStore.Partitioning.Partitioner;

namespace SAFE.EventStore.Services
{
    /// <summary>
    /// This EventSourcing protocol
    /// stores references to ImmutableData
    /// in the entries of a MutableData
    /// respresenting a stream.
    /// 
    /// TODO: Implement shared access to mds between apps.
    /// </summary>
    public class EventStoreImDProtocol : IDisposable, IEventStore
    {
        const int MD_ENTRIES_COUNT = 995;

        const string VERSION_KEY = "version";
        const string METADATA_KEY = "metadata";
        const string PROTOCOL = "IData";

        readonly string _protocolId = $"{PROTOCOL}/";
        readonly string AppContainerPath;

        string DbIdForProtocol(string databaseId)
        {
            return $"{_protocolId}{databaseId}";
        }

        #region Init

        Session _session;
        MDataInfoActions _mDataInfo;
        MData _mData;
        MDataPermissions _mDataPermissions;
        Crypto _crypto;
        AccessContainer _accessContainer;
        MDataEntryActions _mDataEntryActions;
        MDataEntries _mDataEntries;
        IData _iData;
        CipherOpt _cipherOpt;

        public EventStoreImDProtocol(string appId, Session session)
        {
            AppContainerPath = $"apps/{appId}";

            _session = session;
            _mDataInfo = session.MDataInfoActions;
            _mData = session.MData;
            _mDataPermissions = session.MDataPermissions;
            _crypto = session.Crypto;
            _accessContainer = session.AccessContainer;
            _mDataEntryActions = session.MDataEntryActions;
            _mDataEntries = session.MDataEntries;
            _iData = session.IData;
            _cipherOpt = session.CipherOpt;
        }

        public void Dispose()
        {
            FreeState();
            GC.SuppressFinalize(this);
        }

        ~EventStoreImDProtocol()
        {
            FreeState();
        }

        void FreeState()
        {
            _session?.Dispose();
            _session = null;
        }

        #endregion Init

        public async Task CreateDbAsync(string databaseId)
        {
            databaseId = DbIdForProtocol(databaseId);

            if (databaseId.Contains(".") || databaseId.Contains("@"))
                throw new NotSupportedException("Unsupported characters '.' and '@'.");

            // Create Permissions
            using (var permissionsHandle = await _mDataPermissions.NewAsync())
            {
                using (var appSignPkH = await _crypto.AppPubSignKeyAsync())
                {
                    await _mDataPermissions.InsertAsync(permissionsHandle, appSignPkH, GetFullPermissions());
                }

                // Create one Md for holding category partition info, and one for index info
                var categoriesInfoBytes = await GetSerialisedMdInfo(permissionsHandle);
                //var partitionIndexInfoBytes = await GetSerialisedMdInfo(permissionsHandle);

                // Finally update App Container (store db info to it)
                var database = new Database
                {
                    DbId = databaseId,
                    Categories = new DataArray { Type = "Buffer", Data = categoriesInfoBytes }, // Points to Md holding categories
                };

                var serializedDb = JsonConvert.SerializeObject(database);
                var appContainer = await _accessContainer.GetMDataInfoAsync(AppContainerPath);
                var dbIdCipherBytes = await _mDataInfo.EncryptEntryKeyAsync(appContainer, database.DbId.ToUtfBytes());
                var dbCipherBytes = await _mDataInfo.EncryptEntryValueAsync(appContainer, serializedDb.ToUtfBytes());
                using (var appContEntryActionsH = await _mDataEntryActions.NewAsync())
                {
                    await _mDataEntryActions.InsertAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes);
                    await _mData.MutateEntriesAsync(appContainer, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
                }
            }
        }

        async Task<List<byte>> GetSerialisedMdInfo(NativeHandle permissionsHandle)
        {
            var info = await _mDataInfo.RandomPrivateAsync(15001);
            await _mData.PutAsync(info, permissionsHandle, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------
            return await _mDataInfo.SerialiseAsync(info);
        }

        /// <summary>
        /// Retrieves all database ids of the user.
        /// </summary>
        /// <returns>List of database ids.</returns>
        public async Task<List<DatabaseId>> GetDatabaseIdsAsync()
        {
            var dbIds = new List<DatabaseId>();
            var appContainerInfo = await _accessContainer.GetMDataInfoAsync(AppContainerPath);
            var appContEntryKeys = await _mData.ListKeysAsync(appContainerInfo);

            foreach (var cipherTxtEntryKey in appContEntryKeys)
            {
                try
                {
                    var plainTxtEntryKey = await _mDataInfo.DecryptAsync(appContainerInfo, cipherTxtEntryKey.Val);
                    var databaseId = plainTxtEntryKey.ToUtfString();
                        
                    if (!databaseId.Contains(_protocolId))
                        continue;
                    dbIds.Add(new DatabaseId(databaseId.Replace(_protocolId, string.Empty)));
                }
                catch (Exception)
                {
                    // We're ignoring any entries we cannot parse just so we can work with the valid entries.
                    // ignored
                }
            }
            return dbIds;
        }

        /// <summary>
        /// Retrieves all categories in the specific database.
        /// </summary>
        /// <param name="databaseId">The database to search in.</param>
        /// <returns>List of the categories in the database.</returns>
        public async Task<List<string>> GetCategoriesAsync(string databaseId)
        {
            var database = await GetDataBase(databaseId);

            var categories = new ConcurrentBag<string>();
            var categoryIndexInfo = await _mDataInfo.DeserialiseAsync(database.Categories.Data);
            var categoryIndexInfoKeys = await _mData.ListKeysAsync(categoryIndexInfo);

            Parallel.ForEach(categoryIndexInfoKeys, key =>
            {
                categories.Add(key.Val.ToUtfString());
            });
            
            return categories.ToList();
        }

        /// <summary>
        ///
        /// 
        /// Retrieves all stream keys of the category, 
        /// found in the specific database.
        /// </summary>
        /// <param name="databaseId">The databae to search in.</param>
        /// <param name="category">The name of the category whose instances we want the keys of.</param>
        /// <returns>List of all streamkeys of the stream type</returns>
        public async Task<List<string>> GetStreamKeysAsync(string databaseId, string category)
        {
            var streamKeys = new ConcurrentBag<string>();
            var categoriesIndexInfo = await GetCategoryMdInfo(databaseId, category);

            var categoryIndexKeys = await _mData.ListKeysAsync(categoriesIndexInfo);
            var partitionKeys = categoryIndexKeys
                .Select(key => key.Val)
                .ToList();

            var tasks = partitionKeys.Select(async key =>
            {
                var categoryInfoBytes = (await _mData.GetValueAsync(categoriesIndexInfo, key)).Item1;
                var categoryInfo = await _mDataInfo.DeserialiseAsync(categoryInfoBytes);
                var categoryEntryKeys = await _mData.ListKeysAsync(categoryInfo);
                foreach (var streamKeySource in categoryEntryKeys)
                    streamKeys.Add(streamKeySource.Val.ToUtfString());
            });

            await Task.WhenAll(tasks);

            return streamKeys.ToList();
        }

        ///// <summary>
        ///// Retrieves all stream keys
        ///// found in the specific database.
        ///// </summary>
        ///// <param name="databaseId">The databae to search in.</param>
        ///// <param name="category">The name of the category whose instances we want the keys of.</param>
        ///// <returns>List of all streamkeys of the stream type</returns>
        //public async Task<List<string>> GetAllStreamKeysAsync(string databaseId)
        //{
        //    var database = await GetDataBase(databaseId);
        //    var dbCategoriesEntries = await GetCategoriesEntries(database, category);
        //    var categoryEntry = dbCategoriesEntries.Single(s => s.Item1.ToUtfString() == category);

        //    // The stream type md, whose entries contains all streamKeys of the type 
        //    // (up to 998 though, and then the next 998 can be found when following link in key "next")
        //    var category_MDataInfo = await _mDataInfo.DeserialiseAsync(categoryEntry.Item2);

        //    var category_MDataInfos = await GetAllCategoryInfos(database);

        //    var tasks = category_MDataInfos.Select(async c =>
        //    {
        //        var categoryEntryKeys = await _mData.ListKeysAsync(category_MDataInfo);
        //        return categoryEntryKeys.Select(key => key.Val.ToUtfString()).ToList();
        //    });

        //    var streamKeys = (await Task.WhenAll(tasks)).SelectMany(c => c).ToList();

        //    return streamKeys;
        //}

        /// <summary>
        /// Retrieves the stream,
        /// including all events in it.
        /// </summary>
        /// <param name="databaseId">The databae to search in.</param>
        /// <param name="streamKey">The key to the specific stream instance, (in format [category]@[guid])</param>
        /// <returns>The entire stream with all events.</returns>
        public async Task<(int, ulong)> GetStreamVersionAsync(string databaseId, string streamKey)
        {
            try
            {
                var key = new StreamKey(streamKey);
                var stream_MDataInfo = await GetStreamMdInfo(databaseId, key);
                var streamDataKeys = await _mData.ListKeysAsync(stream_MDataInfo);  // lists all eventbatches stored to this stream instance

                var versionKey = streamDataKeys.First(c => VERSION_KEY == c.Val.ToUtfString());
                var entryVal = await _mData.GetValueAsync(stream_MDataInfo, versionKey.Val);
                var versionString = entryVal.Item1.ToUtfString();
                var version = int.Parse(versionString);
                return (version, entryVal.Item2);
            }
            catch (CategoryNotFoundException ex)
            { return (-1, 0); } // i.e. does not exist
            catch (StreamKeyNotFoundException ex)
            { return (-1, 0); } // i.e. does not exist
        }

        /// <summary>
        /// Retrieves the stream,
        /// including all events in it.
        /// </summary>
        /// <param name="databaseId">The databae to search in.</param>
        /// <param name="streamKey">The key to the specific stream instance, (in format [category]@[guid])</param>
        /// <returns>The entire stream with all events.</returns>
        public async Task<Result<ReadOnlyStream>> GetStreamAsync(string databaseId, string streamKey, int newSinceVersion = -1)
        {
            var key = new StreamKey(streamKey);
            var streamInfo = await GetStreamMdInfo(databaseId, key);
            var dataKeys = await _mData.ListKeysAsync(streamInfo); // get the entries of this specific stream instance

            var bag = new ConcurrentBag<EventBatch>();
            var tasks = dataKeys.Select(async k =>  // foreach event batch in stream
            {
                var eventBatchEntry = await _mData.GetValueAsync(streamInfo, k.Val);
                var dataKey = k.Val.ToUtfString();
                if (METADATA_KEY == dataKey || VERSION_KEY == dataKey)
                    return;

                // only fetch events more recent than version passed as argument
                var versionRange = dataKey.Split('@');
                if (newSinceVersion >= int.Parse(versionRange.Last()))
                    return; // this will speed up retrieval when we have a cached version of the stream locally (as we only request new events since last version)

                var jsonBatch = eventBatchEntry.Item1.ToUtfString();
                var batch = JsonConvert.DeserializeObject<StoredEventBatch>(jsonBatch);
                var eventDataBag = new ConcurrentBag<EventData>();

                var tasksInner = batch.Select(async stored => // foreach event in event batch
                {
                    var eventData = await GetEventDataFromAddress(stored);
                    eventDataBag.Add(eventData);
                });

                await Task.WhenAll(tasksInner);

                bag.Add(new EventBatch(streamKey, eventDataBag.First().MetaData.CausationId, eventDataBag.OrderBy(c => c.MetaData.SequenceNumber).ToList()));
            });
            
            await Task.WhenAll(tasks);

            var batches = bag
                .OrderBy(c => c.Body.First().MetaData.SequenceNumber)
                .ToList();

            if (batches.Count == 0)
                return Result.OK((ReadOnlyStream)new EmptyStream(key.StreamName, key.StreamId));
            else
                return Result.OK((ReadOnlyStream)new PopulatedStream(key.StreamName, key.StreamId, batches)); // also checks integrity of data structure (with regards to sequence nr)
        }

        async Task<Database> GetDataBase(string databaseId)
        {
            databaseId = DbIdForProtocol(databaseId);
            
            List<byte> content;
            var appCont = await _accessContainer.GetMDataInfoAsync(AppContainerPath);
            var dbIdCipherBytes = await _mDataInfo.EncryptEntryKeyAsync(appCont, databaseId.ToUtfBytes());

            try
            {
                var entryValue = await _mData.GetValueAsync(appCont, dbIdCipherBytes);
                var dbCipherBytes = entryValue.Item1;
                content = await _mDataInfo.DecryptAsync(appCont, dbCipherBytes);
                var database = JsonConvert.DeserializeObject<Database>(content.ToUtfString());
                database.Version = entryValue.Item2;
                return database;
            }
            catch // todo: better granularity needed here, since this exception is for failing to get the value only
            {
                throw new DatabaseNotFoundException($"Database id {databaseId} does not exist!");
            }
        }
        
        //async Task<List<MDataInfo>> GetAllCategoryInfos(Database database)
        //{
        //    var dbCatPartitionEntries = new ConcurrentBag<MDataInfo>();
        //    var categoriesIndexInfo = await _mDataInfo.DeserialiseAsync(database.Categories.Data);
        //    var categoriesIndexInfoKeys = await _mData.ListKeysAsync(categoriesIndexInfo);

        //    var tasks = categoriesIndexInfoKeys.Select(async c =>
        //    {
        //        var categoryInfoBytes = (await _mData.GetValueAsync(categoriesIndexInfo, c.Val)).Item1;
        //        dbCatPartitionEntries.Add(await _mDataInfo.DeserialiseAsync(categoryInfoBytes));
        //    });

        //    await Task.WhenAll(tasks);

        //    return dbCatPartitionEntries.ToList();
        //}

        //// very heavy
        //async Task<List<(List<byte>, List<byte>, ulong)>> GetAllCategoriesEntries(Database database)
        //{
        //    var dbCatPartitionEntries = new ConcurrentBag<(List<byte>, List<byte>, ulong)>();
        //    var dbCatPartitionDataInfo = await _mDataInfo.DeserialiseAsync(database.CategoryPartitionIndex.Data);
        //    var dbCatPartitionDataKeys = await _mData.ListKeysAsync(dbCatPartitionDataInfo);

        //    var tasks = dbCatPartitionDataKeys.Select(async c =>
        //    {
        //        var partition = await _mData.GetValueAsync(dbCatPartitionDataInfo, c.Val);
        //        var categories = await GetCategories(partition.Item1);
        //        Parallel.ForEach(categories, entry =>
        //        {
        //            dbCatPartitionEntries.Add((entry.Item1, entry.Item2, entry.Item3));
        //        });
        //    });

        //    await Task.WhenAll(tasks);

        //    return dbCatPartitionEntries.ToList();
        //}

        public class StreamKey
        {
            public StreamKey(string streamKey)
            {
                var (streamName, streamId) = GetKeyParts(streamKey);
                Key = streamKey;
                StreamName = streamName;
                StreamId = streamId;
            }

            public string Key { get; private set; }
            public string StreamName { get; private set; }
            public long StreamId { get; private set; }

            (string, long) GetKeyParts(string streamKey)
            {
                var source = streamKey.Split('@');
                var streamName = source[0];
                var streamId = long.Parse(source[1]);
                return (streamName, streamId);
            }
        }

        async Task<MDataInfo> GetCategoryMdInfo(string databaseId, string streamName)
        {
            try
            {
                var database = await GetDataBase(databaseId);
                var categoryIndexInfo = await _mDataInfo.DeserialiseAsync(database.Categories.Data);
                var categoryMdInfoBytes = await _mData.GetValueAsync(categoryIndexInfo, streamName.ToUtfBytes());
                var categoryMdInfo = await _mDataInfo.DeserialiseAsync(categoryMdInfoBytes.Item1);
                return categoryMdInfo;
            }
            catch (DatabaseNotFoundException)
            { throw; }
            catch // todo: catch correct exception
            { throw new CategoryNotFoundException($"Category {streamName} does not exist!"); }
        }

        async Task<MDataInfo> GetStreamMdInfo(string databaseId, StreamKey streamKey)
        {
            var categoryMdInfo = await GetCategoryMdInfo(databaseId, streamKey.StreamName);

            try
            {
                var streamMdInfo = await GetPartitionedMdInfo(categoryMdInfo, streamKey.Key, streamKey.Key);
                return streamMdInfo;
            }
            catch (PartitionMdNotFoundException)
            { throw new StreamKeyPartitionNotFoundException($"Partition for stream {streamKey} does not exist!"); }
            catch (DataMdNotFoundException)
            { throw new StreamKeyNotFoundException($"Stream {streamKey.Key} does not exist!"); } //return Result.Fail<ReadOnlyStream>("Stream does not exist!"); 
        }

        // used for both category and streamkey partitions
        async Task<MDataInfo> GetPartitionedMdInfo(MDataInfo partitionIndexInfo, string partitionKeySource, string mdKey)
        {
            var partitionKeySourceBytes = partitionKeySource.ToUtfBytes();
            var partitionInfo = await GetMdPartitionInfo(partitionIndexInfo, partitionKeySourceBytes);
            try
            {
                var mdKeyBytes = mdKey.ToUtfBytes();
                var mdRef = await _mData.GetValueAsync(partitionInfo, mdKeyBytes);
                var mdInfo = await _mDataInfo.DeserialiseAsync(mdRef.Item1);
                return mdInfo;
            }
            catch
            {
                throw new DataMdNotFoundException();
            }
        }

        // used for both category and streamkey partitions
        async Task<MDataInfo> GetMdPartitionInfo(MDataInfo partitionIndexInfo, List<byte> keyBytes)
        {
            try
            {
                var partitionKey = Partitioner.GetPartition(keyBytes, MD_ENTRIES_COUNT).ToString();
                var partitionKeyBytes = partitionKey.ToUtfBytes();
                var mdPartitionRef = await _mData.GetValueAsync(partitionIndexInfo, partitionKeyBytes);
                var mdPartitionInfo = await _mDataInfo.DeserialiseAsync(mdPartitionRef.Item1);
                return mdPartitionInfo;
            }
            catch
            {
                throw new PartitionMdNotFoundException();
            }
        }


        async Task<(string, List<byte>)> CreatePartition(StreamKey key, List<byte> serializedStream_MdInfo)
        {
            var partitionKey = Partitioner.GetPartition(key.Key, MD_ENTRIES_COUNT).ToString();

            var partitionData = new Dictionary<string, List<byte>>
            {
                {
                    METADATA_KEY, new MDMetaData
                    {
                        { "type", "partition" },
                        { "partitionKey", partitionKey }
                    }.Json().ToUtfBytes()
                },
                {
                    key.Key,
                    serializedStream_MdInfo
                }
            };

            var partitionInfoBytes = await CreateMd(partitionData);

            return (partitionKey, partitionInfoBytes);
        }

        async Task CreateCategory(string databaseId, StreamKey key, List<byte> serializedStream_MdInfo)
        {
            var (partitionKey, partitionInfoBytes) = await CreatePartition(key, serializedStream_MdInfo);
            var categoryData = new Dictionary<string, List<byte>>
            {
                {
                    METADATA_KEY, new MDMetaData
                    {
                        { "type", "category" },
                        { "typeName", key.StreamName }
                    }.Json().ToUtfBytes()
                },
                {
                    partitionKey,
                    partitionInfoBytes
                }
            };

            var categoryInfoBytes = await CreateMd(categoryData);

            var db = await GetDataBase(databaseId);
            var categoryIndexInfo = await _mDataInfo.DeserialiseAsync(db.Categories.Data);
            using (var indexEntryActionsH = await _mDataEntryActions.NewAsync())
            {
                // create the insert action
                await _mDataEntryActions.InsertAsync(indexEntryActionsH, key.StreamName.ToUtfBytes(), categoryInfoBytes);
                await _mData.MutateEntriesAsync(categoryIndexInfo, indexEntryActionsH); // <----------------------------------------------    Commit ------------------------
            }
        }

        async Task<List<byte>> CreateMd(Dictionary<string, List<byte>> data)
        {
            using (var permissionH = await _mDataPermissions.NewAsync())
            {
                using (var appSignPkH = await _crypto.AppPubSignKeyAsync())
                    await _mDataPermissions.InsertAsync(permissionH, appSignPkH, GetFullPermissions());

                using (var dataEntries = await _mDataEntries.NewAsync())
                {
                    await SetDataEntries(dataEntries, data);
                    return await GetSerialisedMdInfo(permissionH, dataEntries);
                }
            }
        }

        async Task<EventData> GetEventDataFromAddress(StoredEvent stored)
        {
            using (var seReaderHandle = await _iData.FetchSelfEncryptorAsync(stored.DataMapAddress.ToArray()))
            {
                var len = await _iData.SizeAsync(seReaderHandle);
                var readData = await _iData.ReadFromSelfEncryptorAsync(seReaderHandle, 0, len);

                var eventData = new EventData(readData.ToArray(),
                    stored.MetaData.CorrelationId,
                    stored.MetaData.CausationId,
                    stored.MetaData.EventClrType,
                    stored.MetaData.Id,
                    stored.MetaData.Name,
                    stored.MetaData.SequenceNumber,
                    stored.MetaData.TimeStamp);

                return eventData;
            }
        }

        /// <summary>
        /// Stores a batch to the stream.
        /// Will protect stream integrity
        /// with regards to version.
        /// 
        /// TODO: We need to return some richer model
        /// so that the outer scope can distinguish between version exception
        /// and other exceptions. This is needed so that ouoter scope can 
        /// load the new events, apply to state and retry the cmd.
        /// </summary>
        /// <param name="databaseId"></param>
        /// <param name="streamKey"></param>
        /// <param name="batch"></param>
        /// <returns></returns>
        public async Task<Result<bool>> StoreBatchAsync(string databaseId, EventBatch batch)
        {
            // Since the streams only have insert permissions,
            // the version of it will increase in a deterministic manner,
            // and we can use the sequenceNr of last event in batch, to derive
            // a version number to supply to the network when mutating the MD.
            try
            {
                var (streamVersion, mdEntryVersion) = await GetStreamVersionAsync(databaseId, batch.StreamKey);
                if (streamVersion == -1)
                    await CreateNewStreamAsync(databaseId, batch);
                else
                {
                    var expectedVersion = batch.Body.First().MetaData.SequenceNumber - 1;
                    if (streamVersion != expectedVersion)
                        throw new InvalidOperationException($"Concurrency exception! Expected stream version {expectedVersion}, but found {streamVersion}.");

                    return await StoreToExistingStream(databaseId, batch, mdEntryVersion); // todo: distinguish MD version exception result from other errors
                }

                return Result.OK(true);
            }
            catch (Exception ex)
            {
                return Result.Fail<bool>(ex.Message);
            }
        }

        async Task CreateNewStreamAsync(string databaseId, EventBatch initBatch)
        {
            if (initBatch.Body.First().MetaData.SequenceNumber != 0)
                throw new InvalidOperationException("First event in a new stream must start with sequence Nr 0!");

            var key = new StreamKey(initBatch.StreamKey);

            var categoryExists = true;
            var streamPartitionExists = true;
            try
            {
                var existing = await GetStreamMdInfo(databaseId, key);
                throw new InvalidOperationException("Stream already exists!");
            }
            catch (CategoryNotFoundException)
            {
                categoryExists = false;
                streamPartitionExists = false;
            }
            catch (StreamKeyPartitionNotFoundException)
            {
                streamPartitionExists = false;
            }
            catch (StreamKeyNotFoundException)
            { }
            
            var jsonBatch = await GetJsonBatch(initBatch);

            var streamData = new Dictionary<string, List<byte>>
            {
                {
                    METADATA_KEY, new MDMetaData
                    {
                        { "type", "stream" },
                        { "streamName", key.StreamName },
                        { "streamId", key.StreamId.ToString() },
                    }.Json().ToUtfBytes()
                },
                {
                    GetBatchKey(initBatch),
                    jsonBatch.ToUtfBytes()
                },
                {
                    VERSION_KEY,
                    initBatch.Body.Last().MetaData.SequenceNumber.ToString().ToUtfBytes()
                }
            };

            var streamInfoBytes = await CreateMd(streamData);

            if (categoryExists)
            {
                var categoryInfo = await GetCategoryMdInfo(databaseId, key.StreamName); // if exists, we mutate it

                if (streamPartitionExists)
                {
                    var partitionInfo = await GetMdPartitionInfo(categoryInfo, key.Key.ToUtfBytes());
                    using (var actionHandle = await _mDataEntryActions.NewAsync()) // create the insert action
                    {
                        await _mDataEntryActions.InsertAsync(actionHandle, initBatch.StreamKey.ToUtfBytes(), streamInfoBytes);
                        await _mData.MutateEntriesAsync(partitionInfo, actionHandle); // <----------------------------------------------    Commit ------------------------
                    }
                }
                else
                {
                    var (partitionKey, partitionInfoBytes) = await CreatePartition(key, streamInfoBytes);

                    using (var actionHandle = await _mDataEntryActions.NewAsync()) // create the insert action
                    {
                        await _mDataEntryActions.InsertAsync(actionHandle, partitionKey.ToUtfBytes(), partitionInfoBytes);
                        await _mData.MutateEntriesAsync(categoryInfo, actionHandle); // <----------------------------------------------    Commit ------------------------
                        return;
                    }
                }
            }
            else
                await CreateCategory(databaseId, key, streamInfoBytes);
        }

        PermissionSet GetFullPermissions()
        {
            return new PermissionSet
            {
                Delete = true,
                Insert = true,
                ManagePermissions = true,
                Read = true,
                Update = true
            };
        }

        async Task SetDataEntries(NativeHandle stream_EntriesH, Dictionary<string, List<byte>> data)
        {
            foreach (var pair in data)
                await _mDataEntries.InsertAsync(stream_EntriesH, pair.Key.ToUtfBytes(), pair.Value);
        }

        async Task<List<byte>> GetSerialisedMdInfo(NativeHandle permissionsHandle, NativeHandle dataEntries)
        {
            var info = await _mDataInfo.RandomPrivateAsync(15001);
            await _mData.PutAsync(info, permissionsHandle, dataEntries); // <----------------------------------------------    Commit ------------------------
            return await _mDataInfo.SerialiseAsync(info);
        }

        async Task<Result<bool>> StoreToExistingStream(string databaseId, EventBatch batch, ulong mdEntryVersion)
        {
            // With ImD-Protocol, the entries of a stream, are the 
            // serialized batch of addresses to ImmutableData
            // which hold the actual event.
            var stream_MDataInfo = await GetStreamMdInfo(databaseId, new StreamKey(batch.StreamKey));
            var batchKey = GetBatchKey(batch);
            var jsonBatch = await GetJsonBatch(batch); // NB: stores to Immutable data!
            var newStreamVersion = batch.Body.Last().MetaData.SequenceNumber.ToString().ToUtfBytes();
            var newMdEntryVersion = mdEntryVersion + 1;
            using (var streamEntryActionsH = await _mDataEntryActions.NewAsync())
            {
                // create the insert action
                await _mDataEntryActions.InsertAsync(streamEntryActionsH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());
                await _mDataEntryActions.UpdateAsync(streamEntryActionsH, VERSION_KEY.ToUtfBytes(), newStreamVersion, newMdEntryVersion);
                // mdEntryVersion gives proper concurrency management of writes to streams.
                // We can now have multiple concurrent processes writing to the same stream and maintain version sequence intact

                try
                {
                    // Finally update md (store batch to it)
                    await _mData.MutateEntriesAsync(stream_MDataInfo, streamEntryActionsH); // <----------------------------------------------    Commit ------------------------
                }
                catch(Exception ex)
                {
                    if (ex.Message.Contains("InvalidSuccessor"))
                    {
                        var (_, mdVersion) = await GetStreamVersionAsync(databaseId, batch.StreamKey);
                        return Result.Fail<bool>($"Concurrency exception! Expected MD entry version {mdEntryVersion} is not valid. Current version is {mdVersion}.");
                    }
                    return Result.Fail<bool>(ex.Message);
                }
                // the exception thrown from MutateEntriesAsync when wrong mdEntryVersion entered (i.e. stream modified since last read)
                // will have to be handled by the outer scope. Preferred way is to load latest version of stream (i.e. load new events)
                // and then apply the cmd to the aggregate again. This becomes a bit of a problem in case of doing outgoing requests from within the aggregate
                // as we might not want to do those requests again (depending on their idempotency at the remote recipient).
                // implementing logic where an outbound non-idempotent request has to be done, will be risky unless managed.
                // currently not aware of a well established pattern for this
                // one way could be to run it over multiple cmds, so that we first set the AR to be in a receiving state, 
                // => (SetReceiving), from here it should reject additional SetReceiving cmds, and only accept 
                // the cmd which performs the outbound request, if it is issued by the same causer (i.e. making it single threaded).
            }

            return Result.OK(true);
        }

        // reconsider method name here, as it currently is obfuscating the fact that
        // state is written to the network with this action (ImD created).
        async Task<string> GetJsonBatch(EventBatch batch)
        {
            ConcurrentBag<object> imd = new ConcurrentBag<object>();

            var tasks = batch.Body.Select(x =>
                Task.Run(async () =>
                {
                    imd.Add(new StoredEvent
                    {
                        MetaData = x.MetaData,
                        DataMapAddress = await StoreImmutableData(x.Payload)
                    });
                }));

            await Task.WhenAll(tasks);

            var jsonBatch = imd.ToList().Json(); // to list might not be necessary here

            return jsonBatch;
        }

        // returns data map address
        async Task<byte[]> StoreImmutableData(byte[] payload)
        {
            using (var cipherOptHandle = await _cipherOpt.NewPlaintextAsync())
            {
                using (var seWriterHandle = await _iData.NewSelfEncryptorAsync())
                {
                    await _iData.WriteToSelfEncryptorAsync(seWriterHandle, payload.ToList());
                    var dataMapAddress = await _iData.CloseSelfEncryptorAsync(seWriterHandle, cipherOptHandle);
                    return dataMapAddress;
                }
            }
        }


        #region Helpers

        class MDMetaData : Dictionary<string, string>
        { }

        class StoredEventBatch : List<StoredEvent>
        { }

        class StoredEvent
        {
            public MetaData MetaData { get; set; }
            public byte[] DataMapAddress { get; set; }
        }

        async Task<List<byte>> GetMdXorName(string plainTextId)
        {
            return await Crypto.Sha3HashAsync(plainTextId.ToUtfBytes());
        }

        string GetBatchKey(EventBatch batch)
        {
            var batchKey = $"{batch.Body.First().MetaData.SequenceNumber}@{batch.Body.Last().MetaData.SequenceNumber}";
            return batchKey;
        }

        //(string, Guid) GetKeyParts(string streamKey)
        //{
        //    var source = streamKey.Split('@');
        //    var streamName = source[0];
        //    var streamId = new Guid(source[1]);
        //    return (streamName, streamId);
        //}

        (string, long) GetKeyParts(string streamKey)
        {
            var source = streamKey.Split('@');
            var streamName = source[0];
            var streamId = long.Parse(source[1]);
            return (streamName, streamId);
        }

        async Task<(byte[], byte[])> GenerateRandomKeyPair()
        {
            var randomKeyPairTuple = await _crypto.EncGenerateKeyPairAsync();
            byte[] encPublicKey, encSecretKey;
            using (var inboxEncPkH = randomKeyPairTuple.Item1)
            {
                using (var inboxEncSkH = randomKeyPairTuple.Item2)
                {
                    encPublicKey = await _crypto.EncPubKeyGetAsync(inboxEncPkH);
                    encSecretKey = await _crypto.EncSecretKeyGetAsync(inboxEncSkH);
                }
            }
            return (encPublicKey, encSecretKey);
        }

        #endregion Helpers
    }
}