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
using SAFE.EventSourcing.Models;
using SAFE.EventSourcing;

namespace SAFE.EventStore.Services
{
    /// <summary>
    /// This EventSourcing protocol
    /// stores serialized events in 
    /// the entries of a MutableData
    /// representing a stream.
    /// </summary>
    public class EventStoreMDProtocol : IDisposable, IEventStore
    {
        const string METADATA_KEY = "metadata";
        const string PROTOCOL = "MData";

        readonly string _protocolId = $"{PROTOCOL}/";

        readonly string AppContainerPath;

        string DbIdForProtocol(string databaseId)
        {
            return $"{_protocolId}{databaseId}";
        }
        #region Init

        Session _session;
        MDataInfo _mDataInfo;
        MData _mData;
        MDataPermissionSet _mDataPermissionSet;
        MDataPermissions _mDataPermissions;
        Crypto _crypto;
        AccessContainer _accessContainer;
        MDataEntryActions _mDataEntryActions;
        MDataKeys _mDataKeys;
        MDataEntries _mDataEntries;

        public EventStoreMDProtocol(string appId, Session session)
        {
            AppContainerPath = $"apps/{appId}";

            _session = session;
            _mDataInfo = new MDataInfo(session);
            _mData = new MData(session);
            _mDataPermissionSet = new MDataPermissionSet(session);
            _mDataPermissions = new MDataPermissions(session);
            _crypto = new Crypto(session);
            _accessContainer = new AccessContainer(session);
            _mDataEntryActions = new MDataEntryActions(session);
            _mDataKeys = new MDataKeys(session);
            _mDataEntries = new MDataEntries(session);
        }

        public void Dispose()
        {
            FreeState();
            GC.SuppressFinalize(this);
        }

        ~EventStoreMDProtocol()
        {
            FreeState();
        }

        void FreeState()
        {
            _session.FreeApp();
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
            using (var dstPubIdMDataInfoH = await _mDataInfo.NewPublicAsync(dstPubIdDigest, 15001))
            {
                var accountExists = false;
                try
                {
                    var keysH = await _mData.ListKeysAsync(dstPubIdMDataInfoH);
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
            using (var categorySelfPermSetH = await _mDataPermissionSet.NewAsync())
            {
                await Task.WhenAll(
                    _mDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kInsert),
                    _mDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kUpdate),
                    _mDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kDelete),
                    _mDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kManagePermissions));

                using (var streamTypesPermH = await _mDataPermissions.NewAsync())
                {
                    using (var appSignPkH = await _crypto.AppPubSignKeyAsync())
                    {
                        await _mDataPermissions.InsertAsync(streamTypesPermH, appSignPkH, categorySelfPermSetH);
                    }

                    // Create Md for holding categories
                    var categoriesMDataInfoH = await _mDataInfo.RandomPrivateAsync(15001);
                    await _mData.PutAsync(categoriesMDataInfoH, streamTypesPermH, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------

                    var serializedCategoriesMdInfo = await _mDataInfo.SerialiseAsync(categoriesMDataInfoH);

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
                    using (var appContH = await _accessContainer.GetMDataInfoAsync(AppContainerPath)) // appContainerHandle
                    {
                        var dbIdCipherBytes = await _mDataInfo.EncryptEntryKeyAsync(appContH, database.DbId.ToUtfBytes());
                        var dbCipherBytes = await _mDataInfo.EncryptEntryValueAsync(appContH, serializedDb.ToUtfBytes());
                        using (var appContEntryActionsH = await _mDataEntryActions.NewAsync())
                        {
                            await _mDataEntryActions.InsertAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes);
                            await _mData.MutateEntriesAsync(appContH, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
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
            using (var appContH = await _accessContainer.GetMDataInfoAsync(AppContainerPath))
            {
                List<List<byte>> cipherTxtEntryKeys;
                using (var appContEntryKeysH = await _mData.ListKeysAsync(appContH))
                {
                    cipherTxtEntryKeys = await _mDataKeys.ForEachAsync(appContEntryKeysH);
                }

                foreach (var cipherTxtEntryKey in cipherTxtEntryKeys)
                {
                    try
                    {
                        var plainTxtEntryKey = await _mDataInfo.DecryptAsync(appContH, cipherTxtEntryKey);
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
        /// <param name="databaseId">The databae to search in.</param>
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
            using (var category_MDataInfoH = await _mDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryEntryKeysH = await _mData.ListKeysAsync(category_MDataInfoH))
                {
                    var streamKeys = await _mDataKeys.ForEachAsync(categoryEntryKeysH);

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
        public async Task<Result<ReadOnlyStream>> GetStreamAsync(string databaseId, string streamKey, int newSinceVersion = -1)
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
            using (var category_MDataInfoH = await _mDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryDataEntH = await _mData.ListEntriesAsync(category_MDataInfoH))  // get the entries of this specific category
                {
                    var streams = await _mDataEntries.ForEachAsync(categoryDataEntH); // lists all instances of this category (key: streamKey, value: serialized mdata info handle)

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

            using (var stream_MDataInfoH = await _mDataInfo.DeserialiseAsync(streamEntry.Item2))
            {
                using (var streamDataEntH = await _mData.ListEntriesAsync(stream_MDataInfoH)) // get the entries of this specific stream instance
                {
                    var eventBatchEntries = await _mDataEntries.ForEachAsync(streamDataEntH); // lists all eventbatches stored to this stream instance

                    var nonBatchKeys = new List<string> { "type", "streamName", "streamId" };

                    var bag = new ConcurrentBag<EventBatch>();
                    var tasks = eventBatchEntries.Select(eventBatchEntry =>
                        Task.Run(() =>
                        {
                            var key = eventBatchEntry.Item1.ToUtfString();

                            if (nonBatchKeys.Contains(key))
                                return;

                            // only fetch events more recent than version passed as argument
                            var versionRange = key.Split('@');
                            if (newSinceVersion >= int.Parse(versionRange.Last()))
                                return; // this will speed up retrieval when we have a cached version of the stream locally (as we only request new events since last version)

                            var jsonBatch = eventBatchEntry.Item2.ToUtfString();
                            var batch = JsonConvert.DeserializeObject<EventBatch>(jsonBatch);
                            bag.Add(batch);
                        }));

                    await Task.WhenAll(tasks);
                    batches.AddRange(bag.OrderBy(c => c.Body.First().MetaData.SequenceNumber));
                }
            }

            if (batches.Count == 0)
                return Result.OK((ReadOnlyStream)new EmptyStream(streamName, streamId));
            else
                return Result.OK((ReadOnlyStream)new PopulatedStream(streamName, streamId, batches)); // also checks integrity of data structure (with regards to sequence nr)
        }

        async Task<Database> GetDataBase(string databaseId)
        {
            databaseId = DbIdForProtocol(databaseId);
            
            List<byte> content;
            using (var appContH = await _accessContainer.GetMDataInfoAsync(AppContainerPath))
            {
                var dbIdCipherBytes = await _mDataInfo.EncryptEntryKeyAsync(appContH, databaseId.ToUtfBytes());
                var entryValue = await _mData.GetValueAsync(appContH, dbIdCipherBytes);
                var dbCipherBytes = entryValue.Item1;

                content = await _mDataInfo.DecryptAsync(appContH, dbCipherBytes);

                var database = JsonConvert.DeserializeObject<Database>(content.ToUtfString());

                database.Version = entryValue.Item2;

                return database;
            }
        }

        async Task<List<(List<byte>, List<byte>, ulong)>> GetCategoriesEntries(Database database)
        {
            List<(List<byte>, List<byte>, ulong)> dbCategoriesEntries;
            using (var dbCategoriesDataInfoH = await _mDataInfo.DeserialiseAsync(database.Categories.Data))
            {
                using (var dbCategoriesDataEntH = await _mData.ListEntriesAsync(dbCategoriesDataInfoH))
                {
                    dbCategoriesEntries = await _mDataEntries.ForEachAsync(dbCategoriesDataEntH);
                }
            }

            return dbCategoriesEntries;
        }


        /// <summary>
        /// Stores a batch to the stream.
        /// Will protect stream integrity
        /// with regards to version.
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
                var checkExists = await GetStreamAsync(databaseId, batch.StreamKey);
                if (checkExists.Error)
                    await CreateNewStreamAsync(databaseId, batch);
                else
                {
                    var currentVersion = checkExists.Value.Data.Last().MetaData.SequenceNumber;
                    var expectedVersion = batch.Body.First().MetaData.SequenceNumber - 1;
                    if (currentVersion != expectedVersion)
                        throw new InvalidOperationException($"Concurrency exception! Expected {expectedVersion}, but found {currentVersion}.");

                    return await StoreToExistingStream(databaseId, batch);
                }

                return Result.OK(true);
            }
            catch (Exception ex)
            {
                return Result.Fail<bool>(ex.Message);
            }
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
            using (var streamSelfPermSetH = await _mDataPermissionSet.NewAsync())
            {
                await Task.WhenAll(
                    _mDataPermissionSet.AllowAsync(streamSelfPermSetH, MDataAction.kInsert),
                    _mDataPermissionSet.AllowAsync(streamSelfPermSetH, MDataAction.kUpdate),
                    _mDataPermissionSet.AllowAsync(streamSelfPermSetH, MDataAction.kDelete),
                    _mDataPermissionSet.AllowAsync(streamSelfPermSetH, MDataAction.kManagePermissions));

                using (var streamPermH = await _mDataPermissions.NewAsync())
                {
                    using (var appSignPkH = await _crypto.AppPubSignKeyAsync())
                    {
                        await _mDataPermissions.InsertAsync(streamPermH, appSignPkH, streamSelfPermSetH);
                    }

                    var (streamName, streamId) = GetKeyParts(initBatch.StreamKey);

                    // Create an MD, with one event batch in it, with entry type of "stream"
                    using (var stream_EntriesH = await _mDataEntries.NewAsync())
                    {
                        var firstEntryInStream1 = "type".ToUtfBytes(); // present in all our mds
                        var firstValueInStream1 = "stream".ToUtfBytes(); // this md is representing a stream, and thus follows a certain set of conventions
                        await _mDataEntries.InsertAsync(stream_EntriesH, firstEntryInStream1, firstValueInStream1);
                        var secondEntryInStream1 = "streamName".ToUtfBytes();
                        var secondValueInStream1 = streamName.ToUtfBytes();
                        await _mDataEntries.InsertAsync(stream_EntriesH, secondEntryInStream1, secondValueInStream1);
                        var thirdEntryInStream1 = "streamId".ToUtfBytes();
                        var thirdValueInStream1 = streamId.ToString().ToUtfBytes(); // .ToString("n")
                        await _mDataEntries.InsertAsync(stream_EntriesH, thirdEntryInStream1, thirdValueInStream1);

                        // First event batch in stream added
                        var batchKey = GetBatchKey(initBatch);
                        var jsonBatch = JsonConvert.SerializeObject(initBatch);
                        await _mDataEntries.InsertAsync(stream_EntriesH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());

                        #region metadata
                        //var secondEntryInStream1 = "metadata".ToUtfBytes();
                        //var metadata = new Dictionary<string, object>
                        //{
                        //    { "streamName", "streamType1" },
                        //    { "streamId", "0001" }
                        //};
                        //var secondValueInStream1 = JsonConvert.SerializeObject(metadata).ToUtfBytes();
                        #endregion metadata

                        var stream_MDataInfoH = await _mDataInfo.RandomPrivateAsync(15001);
                        await _mData.PutAsync(stream_MDataInfoH, streamPermH, stream_EntriesH); // <----------------------------------------------    Commit ------------------------

                        var serializedStream_MdInfo = await _mDataInfo.SerialiseAsync(stream_MDataInfoH); // Value

                        var existingCategory = categories.SingleOrDefault(s => s.Item1.ToUtfString() == streamName);
                        if (existingCategory.Item1 != null && existingCategory.Item2 != null)
                        {
                            using (var category_MDataInfoH = await _mDataInfo.DeserialiseAsync(existingCategory.Item2))
                            {
                                using (var category_EntriesH = await _mDataEntryActions.NewAsync())
                                {
                                    // create the insert action
                                    await _mDataEntryActions.InsertAsync(category_EntriesH, initBatch.StreamKey.ToUtfBytes(), serializedStream_MdInfo);
                                    await _mData.MutateEntriesAsync(category_MDataInfoH, category_EntriesH); // <----------------------------------------------    Commit ------------------------
                                    return;
                                }
                            }
                        }

                        using (var category_EntriesH_1 = await _mDataEntries.NewAsync())
                        {
                            #region Create Category MD

                            await _mDataEntries.InsertAsync(category_EntriesH_1, "type".ToUtfBytes(), "category".ToUtfBytes());
                            await _mDataEntries.InsertAsync(category_EntriesH_1, "typeName".ToUtfBytes(), streamName.ToUtfBytes());
                            await _mDataEntries.InsertAsync(category_EntriesH_1, initBatch.StreamKey.ToUtfBytes(), serializedStream_MdInfo);

                            var category_MDataInfoH = await _mDataInfo.RandomPrivateAsync(15001);
                            await _mData.PutAsync(category_MDataInfoH, streamPermH, category_EntriesH_1); // <----------------------------------------------    Commit ------------------------

                            var serializedCategory_MdInfo = await _mDataInfo.SerialiseAsync(category_MDataInfoH); // Value

                            #endregion Create Category MD


                            #region Insert new category to Stream Categories Directory MD

                            using (var categoriesMDataInfoH = await _mDataInfo.DeserialiseAsync(database.Categories.Data))
                            {
                                using (var category_EntriesH_2 = await _mDataEntryActions.NewAsync())
                                {
                                    // create the insert action
                                    await _mDataEntryActions.InsertAsync(category_EntriesH_2, streamName.ToUtfBytes(), serializedCategory_MdInfo);
                                    await _mData.MutateEntriesAsync(categoriesMDataInfoH, category_EntriesH_2); // <----------------------------------------------    Commit ------------------------
                                }

                                var serializedCategoriesMdInfo = await _mDataInfo.SerialiseAsync(categoriesMDataInfoH);

                                // Replace the database stream type info with the updated version
                                database.Categories = new DataArray { Type = "Buffer", Data = serializedCategoriesMdInfo }; // Points to Md holding stream types

                                // serialize and encrypt the database
                                var serializedDb = JsonConvert.SerializeObject(database);
                                using (var appContH = await _accessContainer.GetMDataInfoAsync(AppContainerPath)) // appContainerHandle
                                {
                                    var dbIdCipherBytes = await _mDataInfo.EncryptEntryKeyAsync(appContH, database.DbId.ToUtfBytes());
                                    var dbCipherBytes = await _mDataInfo.EncryptEntryValueAsync(appContH, serializedDb.ToUtfBytes());
                                    using (var appContEntryActionsH = await _mDataEntryActions.NewAsync())
                                    {
                                        // create the update action, will fail if the entry was updated from somewhere else since db was fetched. todo: reload the db and apply again
                                        await _mDataEntryActions.UpdateAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes, database.Version + 1);

                                        // Finally update App Container (store new db info to it)
                                        await _mData.MutateEntriesAsync(appContH, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
                                    }
                                }
                            }

                            #endregion Insert new category to Stream Categories Directory MD
                        }
                    }
                }
            }
        }

        async Task<Result<bool>> StoreToExistingStream(string databaseId, EventBatch batch)
        {
            var database = await GetDataBase(databaseId); // Get the database
            var dbCategoriesEntries = await GetCategoriesEntries(database); // Get all categories
            var (streamName, streamId) = GetKeyParts(batch.StreamKey);
            var categoryEntry = dbCategoriesEntries.Single(s => s.Item1.ToUtfString() == streamName);

            // Get all streams of the category
            // The category md, whose entries contains all streamKeys of the category 
            // (up to 998 though, and then the next 998 can be found when following link in key "next")
            (List<byte>, List<byte>, ulong) streamEntry;
            using (var category_MDataInfoH = await _mDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryDataEntH = await _mData.ListEntriesAsync(category_MDataInfoH))  // get the entries of this specific category
                {
                    var streams = await _mDataEntries.ForEachAsync(categoryDataEntH); // lists all instances of this type

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

            using (var stream_MDataInfoH = await _mDataInfo.DeserialiseAsync(streamEntry.Item2))
            {
                // First event batch in stream added
                var batchKey = GetBatchKey(batch);
                var jsonBatch = JsonConvert.SerializeObject(batch);

                using (var streamEntryActionsH = await _mDataEntryActions.NewAsync())
                {
                    // create the insert action
                    await _mDataEntryActions.InsertAsync(streamEntryActionsH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());

                    // Finally update md (store batch to it)
                    await _mData.MutateEntriesAsync(stream_MDataInfoH, streamEntryActionsH); // <----------------------------------------------    Commit ------------------------
                }
            }

            return Result.OK(true);
        }

        #region Helpers

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

        async Task<(List<byte>, List<byte>)> GenerateRandomKeyPair()
        {
            var randomKeyPairTuple = await _crypto.EncGenerateKeyPairAsync();
            List<byte> inboxEncPk, inboxEncSk;
            using (var inboxEncPkH = randomKeyPairTuple.Item1)
            {
                using (var inboxEncSkH = randomKeyPairTuple.Item2)
                {
                    inboxEncPk = await _crypto.EncPubKeyGetAsync(inboxEncPkH);
                    inboxEncSk = await _crypto.EncSecretKeyGetAsync(inboxEncSkH);
                }
            }
            return (inboxEncPk, inboxEncSk);
        }

        #endregion Helpers
    }
}