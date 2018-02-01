using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Utils;
using Newtonsoft.Json;
using SAFE.DotNET.Models;
using SAFE.DotNET.Native;
using SAFE.EventStore.Models;
using SAFE.SystemUtils;
using System.Collections.Concurrent;

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
        const string VERSION_KEY = "version";
        const string METADATA_KEY = "metadata";
        const string PROTOCOL = "IData";

        readonly string _protocolId = $"{PROTOCOL}/";

        readonly string AppContainerPath = $"apps/{AppSession.AppId}";

        string DbIdForProtocol(string databaseId)
        {
            return $"{_protocolId}{databaseId}";
        }
        #region Init

        public EventStoreImDProtocol()
        { }

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
            Session.FreeApp();
        }

        #endregion Init

        async Task<List<byte>> GetMdXorName(string plainTextId)
        {
            return await NativeUtils.Sha3HashAsync(plainTextId.ToUtfBytes());
        }

        // Creates db with address to category MD
        public async Task CreateDbAsync(string databaseId)
        {
            databaseId = DbIdForProtocol(databaseId);

            if (databaseId.Contains(".") || databaseId.Contains("@"))
                throw new NotSupportedException("Unsupported characters '.' and '@'.");

            // Check if account exits first and return error
            var dstPubIdDigest = await GetMdXorName(databaseId);
            using (var dstPubIdMDataInfoH = await MDataInfo.NewPublicAsync(dstPubIdDigest, 15001))
            {
                var accountExists = false;
                try
                {
                    var keysH = await MData.ListKeysAsync(dstPubIdMDataInfoH);
                    keysH.Dispose();
                    accountExists = true;
                }
                catch (Exception)
                {
                    // ignored - acct not found
                }
                if (accountExists)
                {
                    throw new Exception("Id already exists.");
                }
            }

            // Create Self Permissions
            using (var categorySelfPermSetH = await MDataPermissionSet.NewAsync())
            {
                await Task.WhenAll(
                    MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kInsert),
                    MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kUpdate),
                    MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kDelete),
                    MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kManagePermissions));

                using (var streamTypesPermH = await MDataPermissions.NewAsync())
                {
                    using (var appSignPkH = await Crypto.AppPubSignKeyAsync())
                    {
                        await MDataPermissions.InsertAsync(streamTypesPermH, appSignPkH, categorySelfPermSetH);
                    }

                    // Create Md for holding categories
                    var categoriesMDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                    await MData.PutAsync(categoriesMDataInfoH, streamTypesPermH, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------

                    var serializedCategoriesMdInfo = await MDataInfo.SerialiseAsync(categoriesMDataInfoH);

                    // Finally update App Container (store db info to it)
                    var database = new Database
                    {
                        DbId = databaseId,
                        Categories = new DataArray { Type = "Buffer", Data = serializedCategoriesMdInfo }, // Points to Md holding stream types
                                                                                                           //Archive = new DataArray {Type = "Buffer", Data = serializedDatabaseMdInfo},
                                                                                                           //DataEncPk = categoryEncPk.ToHexString(),
                                                                                                           //DataEncSk = categoryEncSk.ToHexString()
                    };

                    var serializedDb = JsonConvert.SerializeObject(database);
                    using (var appContH = await AccessContainer.GetMDataInfoAsync(AppContainerPath)) // appContainerHandle
                    {
                        var dbIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, database.DbId.ToUtfBytes());
                        var dbCipherBytes = await MDataInfo.EncryptEntryValueAsync(appContH, serializedDb.ToUtfBytes());
                        using (var appContEntryActionsH = await MDataEntryActions.NewAsync())
                        {
                            await MDataEntryActions.InsertAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes);
                            await MData.MutateEntriesAsync(appContH, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Retrieves all database ids of the user.
        /// </summary>
        /// <returns>List of database ids.</returns>
        public async Task<List<DatabaseId>> GetDatabaseIdsAsync()
        {
            var dbIds = new List<DatabaseId>();
            using (var appContH = await AccessContainer.GetMDataInfoAsync(AppContainerPath))
            {
                List<List<byte>> cipherTxtEntryKeys;
                using (var appContEntryKeysH = await MData.ListKeysAsync(appContH))
                {
                    cipherTxtEntryKeys = await MDataKeys.ForEachAsync(appContEntryKeysH);
                }

                foreach (var cipherTxtEntryKey in cipherTxtEntryKeys)
                {
                    try
                    {
                        var plainTxtEntryKey = await MDataInfo.DecryptAsync(appContH, cipherTxtEntryKey);
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
            var dbCategoriesEntries = await GetCategoriesEntries(database);

            var categories = dbCategoriesEntries
                .Select(s => s.Item1.ToUtfString())
                .ToList();

            return categories;
        }

        /// <summary>
        /// Retrieves all stream keys of the category, 
        /// found in the specific database.
        /// </summary>
        /// <param name="databaseId">The databae to search in.</param>
        /// <param name="category">The name of the category whose instances we want the keys of.</param>
        /// <returns>List of all streamkeys of the stream type</returns>
        public async Task<List<string>> GetStreamKeysAsync(string databaseId, string category)
        {
            var database = await GetDataBase(databaseId);
            var dbCategoriesEntries = await GetCategoriesEntries(database);
            var categoryEntry = dbCategoriesEntries.Single(s => s.Item1.ToUtfString() == category);

            // The stream type md, whose entries contains all streamKeys of the type 
            // (up to 998 though, and then the next 998 can be found when following link in key "next")
            using (var category_MDataInfoH = await MDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryEntryKeysH = await MData.ListKeysAsync(category_MDataInfoH))
                {
                    var streamKeys = await MDataKeys.ForEachAsync(categoryEntryKeysH);

                    return streamKeys.Select(key => key.ToUtfString()).ToList();
                }
            }
        }

        /// <summary>
        /// Retrieves the stream,
        /// including all events in it.
        /// </summary>
        /// <param name="databaseId">The databae to search in.</param>
        /// <param name="streamKey">The key to the specific stream instance, (in format [category]@[guid])</param>
        /// <returns>The entire stream with all events.</returns>
        public async Task<(int, ulong)> GetStreamVersionAsync(string databaseId, string streamKey)
        {
            var (streamName, streamId) = GetKeyParts(streamKey);
            var batches = new List<EventBatch>();

            var database = await GetDataBase(databaseId);
            var dbCategoriesEntries = await GetCategoriesEntries(database); // Get all categories
            var categoryEntry = dbCategoriesEntries.SingleOrDefault(s => s.Item1.ToUtfString() == streamName);
            if (categoryEntry.Item1 == null || categoryEntry.Item2 == null)
                return (-1, 0); // i.e. does not exist

            // Here we get all streams of the category
            // We get the category md, whose entries contains all streamKeys of the category
            // (up to 998 though, and then the next 998 can be found when following link in key "next")
            (List<byte>, List<byte>, ulong) streamEntry;
            using (var category_MDataInfoH = await MDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryDataEntH = await MData.ListEntriesAsync(category_MDataInfoH))  // get the entries of this specific category
                {
                    var streams = await MDataEntries.ForEachAsync(categoryDataEntH); // lists all instances of this category (key: streamKey, value: serialized mdata info handle)

                    try
                    {
                        streamEntry = streams.First(s => s.Item1.ToUtfString() == streamKey); // find the instance matching this streamKey
                    }
                    catch (InvalidOperationException ex)
                    {
                        return (-1, 0); // i.e. does not exist
                    }
                }
            }

            using (var stream_MDataInfoH = await MDataInfo.DeserialiseAsync(streamEntry.Item2))
            {
                using (var streamDataEntH = await MData.ListEntriesAsync(stream_MDataInfoH)) // get the entries of this specific stream instance
                {
                    var eventBatchEntries = await MDataEntries.ForEachAsync(streamDataEntH); // lists all eventbatches stored to this stream instance
                    
                    var entry = eventBatchEntries.First(e => VERSION_KEY == e.Item1.ToUtfString());
                    var versionString = entry.Item2.ToUtfString();
                    var version = int.Parse(versionString);
                    return (version, entry.Item3);
                }
            }
        }

        /// <summary>
        /// Retrieves the stream,
        /// including all events in it.
        /// </summary>
        /// <param name="databaseId">The databae to search in.</param>
        /// <param name="streamKey">The key to the specific stream instance, (in format [category]@[guid])</param>
        /// <returns>The entire stream with all events.</returns>
        public async Task<Result<ReadOnlyStream>> GetStreamAsync(string databaseId, string streamKey)
        {
            var (streamName, streamId) = GetKeyParts(streamKey);
            var batches = new List<EventBatch>();

            var database = await GetDataBase(databaseId);
            var dbCategoriesEntries = await GetCategoriesEntries(database); // Get all categories
            var categoryEntry = dbCategoriesEntries.SingleOrDefault(s => s.Item1.ToUtfString() == streamName);
            if (categoryEntry.Item1 == null || categoryEntry.Item2 == null)
                return Result.Fail<ReadOnlyStream>("Stream does not exist!");

            // Here we get all streams of the category
            // We get the category md, whose entries contains all streamKeys of the category
            // (up to 998 though, and then the next 998 can be found when following link in key "next")
            (List<byte>, List<byte>, ulong) streamEntry;
            using (var category_MDataInfoH = await MDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryDataEntH = await MData.ListEntriesAsync(category_MDataInfoH))  // get the entries of this specific category
                {
                    var streams = await MDataEntries.ForEachAsync(categoryDataEntH); // lists all instances of this category (key: streamKey, value: serialized mdata info handle)

                    try
                    {
                        streamEntry = streams.First(s => s.Item1.ToUtfString() == streamKey); // find the instance matching this streamKey
                    }
                    catch (InvalidOperationException ex)
                    {
                        return Result.Fail<ReadOnlyStream>("Stream does not exist!");
                    }
                }
            }

            using (var stream_MDataInfoH = await MDataInfo.DeserialiseAsync(streamEntry.Item2))
            {
                using (var streamDataEntH = await MData.ListEntriesAsync(stream_MDataInfoH)) // get the entries of this specific stream instance
                {
                    var eventBatchEntries = await MDataEntries.ForEachAsync(streamDataEntH); // lists all eventbatches stored to this stream instance

                    var bag = new ConcurrentBag<EventBatch>();
                    var tasks1 = eventBatchEntries.Select(eventBatchEntry =>
                        Task.Run(async () => // foreach event batch in stream
                        {
                            var key = eventBatchEntry.Item1.ToUtfString();
                            if (METADATA_KEY == key || VERSION_KEY == key)
                                return;
                            var jsonBatch = eventBatchEntry.Item2.ToUtfString();
                            var batch = JsonConvert.DeserializeObject<StoredEventBatch>(jsonBatch);
                            var eventDataBag = new ConcurrentBag<EventData>();

                            var tasks2 = batch.Select(stored =>
                                Task.Run(async () => // foreach event in event batch
                                {
                                    var eventData = await GetEventDataFromAddress(stored);
                                    eventDataBag.Add(eventData);
                                }));

                            await Task.WhenAll(tasks2);

                            bag.Add(new EventBatch(streamKey, eventDataBag.First().MetaData.CausationId, eventDataBag.OrderBy(c => c.MetaData.SequenceNumber).ToList()));
                        }));

                    await Task.WhenAll(tasks1);
                    batches.AddRange(bag.OrderBy(c => c.Body.First().MetaData.SequenceNumber));
                }
            }

            var stream = new ReadOnlyStream(streamName, streamId, batches); // also checks integrity of data structure (with regards to sequence nr)
            return Result.OK(stream);
        }

        async Task<Database> GetDataBase(string databaseId)
        {
            databaseId = DbIdForProtocol(databaseId);
            
            List<byte> content;
            using (var appContH = await AccessContainer.GetMDataInfoAsync(AppContainerPath))
            {
                var dbIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, databaseId.ToUtfBytes());
                var entryValue = await MData.GetValueAsync(appContH, dbIdCipherBytes);
                var dbCipherBytes = entryValue.Item1;

                content = await MDataInfo.DecryptAsync(appContH, dbCipherBytes);

                var database = JsonConvert.DeserializeObject<Database>(content.ToUtfString());

                database.Version = entryValue.Item2;

                return database;
            }
        }

        async Task<List<(List<byte>, List<byte>, ulong)>> GetCategoriesEntries(Database database)
        {
            List<(List<byte>, List<byte>, ulong)> dbCategoriesEntries;
            using (var dbCategoriesDataInfoH = await MDataInfo.DeserialiseAsync(database.Categories.Data))
            {
                using (var dbCategoriesDataEntH = await MData.ListEntriesAsync(dbCategoriesDataInfoH))
                {
                    dbCategoriesEntries = await MDataEntries.ForEachAsync(dbCategoriesDataEntH);
                }
            }

            return dbCategoriesEntries;
        }

        async Task<EventData> GetEventDataFromAddress(StoredEvent stored)
        {
            using (var seReaderHandle = await IData.FetchSelfEncryptorAsync(stored.DataMapAddress))
            {
                var len = await IData.SizeAsync(seReaderHandle);
                var readData = await IData.ReadFromSelfEncryptorAsync(seReaderHandle, 0, len);

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

        class MDMetaData : Dictionary<string, string>
        {

        }

        /// <summary>
        /// Adds a stream to the database.
        /// </summary>
        /// <param name="databaseId"></param>
        /// <param name="initBatch">The events of the new stream</param>
        /// <returns></returns>
        async Task CreateNewStreamAsync(string databaseId, EventBatch initBatch)
        {
            if (initBatch.Body.First().MetaData.SequenceNumber != 0)
                throw new InvalidOperationException("First event in a new stream must start with sequence Nr 0!");

            // Get the database

            var database = await GetDataBase(databaseId);
            var categories = await GetCategoriesEntries(database);

            // Create Self Permissions to the MDs
            using (var streamSelfPermSetH = await MDataPermissionSet.NewAsync())
            {
                await Task.WhenAll(
                    MDataPermissionSet.AllowAsync(streamSelfPermSetH, MDataAction.kInsert),
                    MDataPermissionSet.AllowAsync(streamSelfPermSetH, MDataAction.kUpdate));

                using (var streamPermH = await MDataPermissions.NewAsync())
                {
                    using (var appSignPkH = await Crypto.AppPubSignKeyAsync())
                    {
                        await MDataPermissions.InsertAsync(streamPermH, appSignPkH, streamSelfPermSetH);
                    }

                    var (streamName, streamId) = GetKeyParts(initBatch.StreamKey);

                    // Create an MD, with one event batch in it, with entry type of "stream"
                    using (var stream_EntriesH = await MDataEntries.NewAsync())
                    {
                        var streamMetadataKey = METADATA_KEY.ToUtfBytes();
                        var streamMetadata = new MDMetaData
                        {
                            { "type", "stream" },
                            { "streamName", streamName },
                            { "streamId", streamId.ToString() },
                        }.Json().ToUtfBytes();
                        await MDataEntries.InsertAsync(stream_EntriesH, streamMetadataKey, streamMetadata);
                        
                        // First event batch in stream added
                        var batchKey = GetBatchKey(initBatch);
                        var jsonBatch = await GetJsonBatch(initBatch); // NB: stores to Immutable data!
                        await MDataEntries.InsertAsync(stream_EntriesH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());

                        var versionKey = VERSION_KEY.ToUtfBytes();
                        await MDataEntries.InsertAsync(stream_EntriesH, versionKey, initBatch.Body.Last().MetaData.SequenceNumber.ToString().ToUtfBytes());

                        var stream_MDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                        await MData.PutAsync(stream_MDataInfoH, streamPermH, stream_EntriesH); // <----------------------------------------------    Commit ------------------------

                        var serializedStream_MdInfo = await MDataInfo.SerialiseAsync(stream_MDataInfoH); // Value

                        var existingCategory = categories.SingleOrDefault(s => s.Item1.ToUtfString() == streamName);
                        if (existingCategory.Item1 != null && existingCategory.Item2 != null)
                        {
                            using (var category_MDataInfoH = await MDataInfo.DeserialiseAsync(existingCategory.Item2))
                            {
                                using (var category_EntriesH = await MDataEntryActions.NewAsync())
                                {
                                    // create the insert action
                                    await MDataEntryActions.InsertAsync(category_EntriesH, initBatch.StreamKey.ToUtfBytes(), serializedStream_MdInfo);
                                    await MData.MutateEntriesAsync(category_MDataInfoH, category_EntriesH); // <----------------------------------------------    Commit ------------------------
                                    return;
                                }
                            }
                        }

                        using (var category_EntriesH_1 = await MDataEntries.NewAsync())
                        {
                            #region Create Category MD
                            var catMetadataKey = METADATA_KEY.ToUtfBytes();
                            var catMetadata = new MDMetaData
                            {
                                { "type", "category" },
                                { "typeName", streamName }
                            }.Json().ToUtfBytes();
                            await MDataEntries.InsertAsync(category_EntriesH_1, catMetadataKey, catMetadata);
                            await MDataEntries.InsertAsync(category_EntriesH_1, initBatch.StreamKey.ToUtfBytes(), serializedStream_MdInfo);

                            var category_MDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                            await MData.PutAsync(category_MDataInfoH, streamPermH, category_EntriesH_1); // <----------------------------------------------    Commit ------------------------

                            var serializedCategory_MdInfo = await MDataInfo.SerialiseAsync(category_MDataInfoH); // Value

                            #endregion Create Category MD


                            #region Insert new category to Stream Categories Directory MD

                            using (var categoriesMDataInfoH = await MDataInfo.DeserialiseAsync(database.Categories.Data))
                            {
                                using (var category_EntriesH_2 = await MDataEntryActions.NewAsync())
                                {
                                    // create the insert action
                                    await MDataEntryActions.InsertAsync(category_EntriesH_2, streamName.ToUtfBytes(), serializedCategory_MdInfo);
                                    await MData.MutateEntriesAsync(categoriesMDataInfoH, category_EntriesH_2); // <----------------------------------------------    Commit ------------------------
                                }

                                var serializedCategoriesMdInfo = await MDataInfo.SerialiseAsync(categoriesMDataInfoH);

                                // Replace the database stream type info with the updated version
                                database.Categories = new DataArray { Type = "Buffer", Data = serializedCategoriesMdInfo }; // Points to Md holding stream types

                                // serialize and encrypt the database
                                var serializedDb = JsonConvert.SerializeObject(database);
                                using (var appContH = await AccessContainer.GetMDataInfoAsync(AppContainerPath)) // appContainerHandle
                                {
                                    var dbIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, database.DbId.ToUtfBytes());
                                    var dbCipherBytes = await MDataInfo.EncryptEntryValueAsync(appContH, serializedDb.ToUtfBytes());
                                    using (var appContEntryActionsH = await MDataEntryActions.NewAsync())
                                    {
                                        // create the update action, will fail if the entry was updated from somewhere else since db was fetched. todo: reload the db and apply again
                                        await MDataEntryActions.UpdateAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes, database.Version + 1);

                                        // Finally update App Container (store new db info to it)
                                        await MData.MutateEntriesAsync(appContH, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
                                    }
                                }
                            }
                            #endregion Insert new category to Stream Categories Directory MD
                        }
                    }
                }
            }
        }

        async Task<Result<bool>> StoreToExistingStream(string databaseId, EventBatch batch, ulong mdEntryVersion)
        {
            var database = await GetDataBase(databaseId); // Get the database
            var dbCategoriesEntries = await GetCategoriesEntries(database); // Get all categories
            var (streamName, streamId) = GetKeyParts(batch.StreamKey);
            var categoryEntry = dbCategoriesEntries.Single(s => s.Item1.ToUtfString() == streamName);

            // Get all streams of the category
            // The category md, whose entries contains all streamKeys of the category 
            // (up to 998 though, and then the next 998 can be found when following link in key "next")
            (List<byte>, List<byte>, ulong) streamEntry;
            using (var category_MDataInfoH = await MDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryDataEntH = await MData.ListEntriesAsync(category_MDataInfoH))  // get the entries of this specific category
                {
                    var streams = await MDataEntries.ForEachAsync(categoryDataEntH); // lists all instances of this type

                    try
                    {
                        streamEntry = streams.First(s => s.Item1.ToUtfString() == batch.StreamKey); // find the instance matching this streamKey
                    }
                    catch (InvalidOperationException ex)
                    {
                        return Result.Fail<bool>("Stream does not exist!");
                    }
                }
            }

            // With ImD-Protocol, the entries of a stream, are the 
            // serialized batch of addresses to ImmutableData
            // which hold the actual event.
            using (var stream_MDataInfoH = await MDataInfo.DeserialiseAsync(streamEntry.Item2))
            {
                var batchKey = GetBatchKey(batch);
                var jsonBatch = await GetJsonBatch(batch); // NB: stores to Immutable data!
                var newStreamVersion = batch.Body.Last().MetaData.SequenceNumber.ToString().ToUtfBytes();
                var newMdEntryVersion = mdEntryVersion + 1;
                using (var streamEntryActionsH = await MDataEntryActions.NewAsync())
                {
                    // create the insert action
                    await MDataEntryActions.InsertAsync(streamEntryActionsH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());
                    await MDataEntryActions.UpdateAsync(streamEntryActionsH, VERSION_KEY.ToUtfBytes(), newStreamVersion, newMdEntryVersion);
                    // mdEntryVersion gives proper concurrency management of writes to streams.
                    // We can now have multiple concurrent processes writing to the same stream and maintain version sequence intact

                    try
                    {
                        // Finally update md (store batch to it)
                        await MData.MutateEntriesAsync(stream_MDataInfoH, streamEntryActionsH); // <----------------------------------------------    Commit ------------------------
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
        async Task<List<byte>> StoreImmutableData(byte[] payload)
        {
            using (var cipherOptHandle = await CipherOpt.NewPlaintextAsync())
            {
                using (var seWriterHandle = await IData.NewSelfEncryptorAsync())
                {
                    await IData.WriteToSelfEncryptorAsync(seWriterHandle, payload.ToList());
                    var dataMapAddress = await IData.CloseSelfEncryptorAsync(seWriterHandle, cipherOptHandle);
                    //await IData.SelfEncryptorWriterFreeAsync(seWriterHandle);
                    return dataMapAddress;
                }
            }
        }

        class StoredEventBatch : List<StoredEvent>
        { }

        class StoredEvent
        {
            public MetaData MetaData { get; set; }
            public List<byte> DataMapAddress { get; set; }
        }

        #region Helpers

        string GetBatchKey(EventBatch batch)
        {
            var batchKey = $"{batch.Body.First().MetaData.SequenceNumber}@{batch.Body.Last().MetaData.SequenceNumber}";
            return batchKey;
        }

        (string, Guid) GetKeyParts(string streamKey)
        {
            var source = streamKey.Split('@');
            var streamName = source[0];
            var streamId = new Guid(source[1]);
            return (streamName, streamId);
        }

        static async Task<(List<byte>, List<byte>)> GenerateRandomKeyPair()
        {
            var randomKeyPairTuple = await Crypto.EncGenerateKeyPairAsync();
            List<byte> inboxEncPk, inboxEncSk;
            using (var inboxEncPkH = randomKeyPairTuple.Item1)
            {
                using (var inboxEncSkH = randomKeyPairTuple.Item2)
                {
                    inboxEncPk = await Crypto.EncPubKeyGetAsync(inboxEncPkH);
                    inboxEncSk = await Crypto.EncSecretKeyGetAsync(inboxEncSkH);
                }
            }
            return (inboxEncPk, inboxEncSk);
        }

        #endregion Helpers
    }
}