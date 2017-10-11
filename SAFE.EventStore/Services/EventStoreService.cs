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

namespace SAFE.EventStore.Services
{
    public class EventStoreService : IDisposable, IEventStoreService
    {
        #region Init
        
        public EventStoreService()
        { }

        public void Dispose()
        {
            FreeState();
            GC.SuppressFinalize(this);
        }

        ~EventStoreService()
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

        // Creates db with example category and 
        // example stream instance in that category with
        // and example event in the stream. 
        // TODO: No examples shall be created here.
        public async Task CreateDbAsync(string databaseId)
        {
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
            using (var categorySelfPermSetH = await MDataPermissions.NewAsync())
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

                    // Create a "stream1" MD, with one event in it, and the default entry "type"
                    using (var stream1_EntriesH = await MDataEntries.NewAsync())
                    {
                        var firstEntryInStream1 = "type".ToUtfBytes(); // present in all our mds
                        var firstValueInStream1 = "stream".ToUtfBytes(); // this md is representing a stream, and thus follows a certain set of conventions
                        await MDataEntries.InsertAsync(stream1_EntriesH, firstEntryInStream1, firstValueInStream1);
                        var secondEntryInStream1 = "streamName".ToUtfBytes();
                        var secondValueInStream1 = "category1".ToUtfBytes();
                        await MDataEntries.InsertAsync(stream1_EntriesH, secondEntryInStream1, secondValueInStream1);
                        var thirdEntryInStream1 = "streamId".ToUtfBytes();
                        var thirdValueInStream1 = "0001".ToUtfBytes(); // 
                        await MDataEntries.InsertAsync(stream1_EntriesH, thirdEntryInStream1, thirdValueInStream1);

                        // First event in stream added
                        var cmdId = Guid.NewGuid();
                        var initBatch = new EventBatch($"{"category1"}@0001", cmdId, new List<EventData> { new EventData(new byte[0], Guid.Empty, cmdId, "mockEventClrType", Guid.NewGuid(), "mockEvent", 0, DateTime.UtcNow) });
                        var jsonBatch = JsonConvert.SerializeObject(initBatch);
                        var batchKey = $"{initBatch.Body.First().SequenceNumber}@{initBatch.Body.Last().SequenceNumber}";
                        await MDataEntries.InsertAsync(stream1_EntriesH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());

                        #region metadata
                        //var secondEntryInStream1 = "metadata".ToUtfBytes();
                        //var metadata = new Dictionary<string, object>
                        //{
                        //    { "streamName", "streamType1" },
                        //    { "streamId", "0001" }
                        //};
                        //var secondValueInStream1 = JsonConvert.SerializeObject(metadata).ToUtfBytes();
                        #endregion metadata

                        var stream1_MDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                        await MData.PutAsync(stream1_MDataInfoH, streamTypesPermH, stream1_EntriesH); // <----------------------------------------------    Commit ------------------------

                        var serializedStream1_MdInfo = await MDataInfo.SerialiseAsync(stream1_MDataInfoH); // Value


                        // Create a "streamType1" MD, with one stream in it ("streamType1@0001")
                        using (var category1_EntriesH = await MDataEntries.NewAsync())
                        {
                            await MDataEntries.InsertAsync(category1_EntriesH, "type".ToUtfBytes(), "category".ToUtfBytes());
                            await MDataEntries.InsertAsync(category1_EntriesH, "typeName".ToUtfBytes(), "category1".ToUtfBytes());
                            var streamKey = "category1@0001";
                            await MDataEntries.InsertAsync(category1_EntriesH, streamKey.ToUtfBytes(), serializedStream1_MdInfo);

                            var category1_MDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                            await MData.PutAsync(category1_MDataInfoH, streamTypesPermH, category1_EntriesH); // <----------------------------------------------    Commit ------------------------

                            var serializedCategory1_MdInfo = await MDataInfo.SerialiseAsync(category1_MDataInfoH); // Value


                            // Create Md for holding stream types, add one example type ("streamType1") which points to an Md which holds all streams of that type
                            var categoriesMDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                            using (var categoriesEntriesH = await MDataEntries.NewAsync())
                            {
                                await MDataEntries.InsertAsync(categoriesEntriesH, "category1".ToUtfBytes(), serializedCategory1_MdInfo);
                                await MData.PutAsync(categoriesMDataInfoH, streamTypesPermH, categoriesEntriesH); // <----------------------------------------------    Commit ------------------------

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
                                using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppSession.AppId)) // appContainerHandle
                                {
                                    var dbIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, databaseId.ToUtfBytes());
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
            using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppSession.AppId))
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
                        dbIds.Add(new DatabaseId(plainTxtEntryKey.ToUtfString()));
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
            List<byte> content;
            using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppSession.AppId))
            {
                var dbIdBytes = databaseId.ToUtfBytes();
                var value = await MData.GetValueAsync(appContH, dbIdBytes);
                content = value.Item1;
            }

            var database = JsonConvert.DeserializeObject<Database>(content.ToUtfString());
            List<(List<byte>, List<byte>, ulong)> dbCategoriesEntries;
            using (var dbCategoriesDataInfoH = await MDataInfo.DeserialiseAsync(database.Categories.Data))
            {
                using (var dbCategoriesDataEntH = await MData.ListEntriesAsync(dbCategoriesDataInfoH))
                {
                    dbCategoriesEntries = await MDataEntries.ForEachAsync(dbCategoriesDataEntH);
                }
            }

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
            List<byte> content;
            using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppSession.AppId))
            {
                var dbIdBytes = databaseId.ToUtfBytes();
                var value = await MData.GetValueAsync(appContH, dbIdBytes);
                content = value.Item1;
            }

            var database = JsonConvert.DeserializeObject<Database>(content.ToUtfString());
            List<(List<byte>, List<byte>, ulong)> dbCategoriesEntries;
            using (var dbCategoriesDataInfoH = await MDataInfo.DeserialiseAsync(database.Categories.Data))
            {
                using (var dbCategoriesDataEntH = await MData.ListEntriesAsync(dbCategoriesDataInfoH))
                {
                    dbCategoriesEntries = await MDataEntries.ForEachAsync(dbCategoriesDataEntH);
                }
            }

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
        public async Task<Result<ReadOnlyStream>> GetStreamAsync(string databaseId, string streamKey)
        {
            var (streamName, streamId) = GetKeyParts(streamKey);
            var batches = new List<EventBatch>();

            // Get the database
            List<byte> content;
            using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppSession.AppId))
            {
                var dbIdBytes = databaseId.ToUtfBytes();
                var value = await MData.GetValueAsync(appContH, dbIdBytes);
                content = value.Item1;
            }

            var database = JsonConvert.DeserializeObject<Database>(content.ToUtfString());

            // Get all stream types
            List<(List<byte>, List<byte>, ulong)> dbCategoriesEntries;
            using (var dbCategoriesDataInfoH = await MDataInfo.DeserialiseAsync(database.Categories.Data))
            {
                using (var dbCategoriesDataEntH = await MData.ListEntriesAsync(dbCategoriesDataInfoH))
                {
                    dbCategoriesEntries = await MDataEntries.ForEachAsync(dbCategoriesDataEntH);
                }
            }

            var categoryEntry = dbCategoriesEntries.Single(s => s.Item1.ToUtfString() == streamName);

            // Get all streams of the type
            // The stream type md, whose entries contains all streamKeys of the type 
            // (up to 998 though, and then the next 998 can be found when following link in key "next")
            (List<byte>, List<byte>, ulong) streamEntry;
            using (var category_MDataInfoH = await MDataInfo.DeserialiseAsync(categoryEntry.Item2))
            {
                using (var categoryDataEntH = await MData.ListEntriesAsync(category_MDataInfoH))  // get the entries of this specific stream type
                {
                    var streams = await MDataEntries.ForEachAsync(categoryDataEntH); // lists all instances of this type

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

                    foreach (var eventBatchEntry in eventBatchEntries)
                    {
                        var jsonBatch = eventBatchEntry.Item2.ToUtfString();
                        var batch = JsonConvert.DeserializeObject<EventBatch>(jsonBatch);
                        batches.Add(batch);
                    }
                }
            }

            var stream = new ReadOnlyStream(streamName, streamId, batches); // also checks integrity of data structure (with regards to sequence nr)
            return Result.OK(stream);
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
                    var currentVersion = checkExists.Value.Data.Last().SequenceNumber;
                    var expectedVersion = batch.Body.First().SequenceNumber - 1;
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
            if (initBatch.Body.First().SequenceNumber != 0)
                throw new InvalidOperationException("First event in a new stream must start with sequence Nr 0!");

            // Get the database
            var dstPubIdDigest = await GetMdXorName(databaseId);
            using (var dstPubIdMDataInfoH = await MDataInfo.NewPublicAsync(dstPubIdDigest, 15001))
            {
                using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppSession.AppId)) // appContainerHandle
                {
                    // Deserialize contents
                    var dbIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, databaseId.ToUtfBytes());
                    var (dbCipherBytes, entryVersion) = await MData.GetValueAsync(dstPubIdMDataInfoH, dbIdCipherBytes);
                    var serializedDb = (await MDataInfo.DecryptAsync(appContH, dbCipherBytes)).ToUtfString();
                    var database = JsonConvert.DeserializeObject<Database>(serializedDb);

                    // Create Self Permissions to the MDs
                    using (var categorySelfPermSetH = await MDataPermissions.NewAsync())
                    {
                        await Task.WhenAll(
                            MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kInsert),
                            MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kUpdate),
                            MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kDelete),
                            MDataPermissionSet.AllowAsync(categorySelfPermSetH, MDataAction.kManagePermissions));

                        using (var categoriesPermH = await MDataPermissions.NewAsync())
                        {
                            using (var appSignPkH = await Crypto.AppPubSignKeyAsync())
                            {
                                await MDataPermissions.InsertAsync(categoriesPermH, appSignPkH, categorySelfPermSetH);
                            }

                            var (streamName, streamId) = GetKeyParts(initBatch.StreamKey);

                            // Create a "stream1" MD, with one event in it, and the default entry "type"
                            using (var stream_EntriesH = await MDataEntries.NewAsync())
                            {
                                var firstEntryInStream1 = "type".ToUtfBytes(); // present in all our mds
                                var firstValueInStream1 = "stream".ToUtfBytes(); // this md is representing a stream, and thus follows a certain set of conventions
                                await MDataEntries.InsertAsync(stream_EntriesH, firstEntryInStream1, firstValueInStream1);
                                var secondEntryInStream1 = "streamName".ToUtfBytes();
                                var secondValueInStream1 = streamName.ToUtfBytes();
                                await MDataEntries.InsertAsync(stream_EntriesH, secondEntryInStream1, secondValueInStream1);
                                var thirdEntryInStream1 = "streamId".ToUtfBytes();
                                var thirdValueInStream1 = streamId.ToString().ToUtfBytes(); // .ToString("n")
                                await MDataEntries.InsertAsync(stream_EntriesH, thirdEntryInStream1, thirdValueInStream1);

                                // First event batch in stream added
                                var jsonBatch = JsonConvert.SerializeObject(initBatch);
                                var batchKey = $"{initBatch.Body.First().SequenceNumber}@{initBatch.Body.Last().SequenceNumber}";
                                await MDataEntries.InsertAsync(stream_EntriesH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());

                                #region metadata
                                //var secondEntryInStream1 = "metadata".ToUtfBytes();
                                //var metadata = new Dictionary<string, object>
                                //{
                                //    { "streamName", "streamType1" },
                                //    { "streamId", "0001" }
                                //};
                                //var secondValueInStream1 = JsonConvert.SerializeObject(metadata).ToUtfBytes();
                                #endregion metadata

                                var stream_MDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                                await MData.PutAsync(stream_MDataInfoH, categoriesPermH, stream_EntriesH); // <----------------------------------------------    Commit ------------------------

                                var serializedStream_MdInfo = await MDataInfo.SerialiseAsync(stream_MDataInfoH); // Value

                                #region Create Category MD
                                using (var category_EntriesH = await MDataEntries.NewAsync())
                                {
                                    await MDataEntries.InsertAsync(category_EntriesH, "type".ToUtfBytes(), "category".ToUtfBytes());
                                    await MDataEntries.InsertAsync(category_EntriesH, "typeName".ToUtfBytes(), streamName.ToUtfBytes());
                                    await MDataEntries.InsertAsync(category_EntriesH, initBatch.StreamKey.ToUtfBytes(), serializedStream_MdInfo);

                                    var category_MDataInfoH = await MDataInfo.RandomPrivateAsync(15001);
                                    await MData.PutAsync(category_MDataInfoH, categoriesPermH, category_EntriesH); // <----------------------------------------------    Commit ------------------------

                                    var serializedCategory_MdInfo = await MDataInfo.SerialiseAsync(category_MDataInfoH); // Value

                                #endregion Create Category MD


                                #region Insert new category to Stream Categories Directory MD

                                var categoriesMDataInfoH = await MDataInfo.DeserialiseAsync(database.Categories.Data);
                                using (var categoriesEntriesH = await MDataEntries.NewAsync())
                                {
                                    await MDataEntries.InsertAsync(categoriesEntriesH, streamName.ToUtfBytes(), serializedCategory_MdInfo);
                                    await MData.PutAsync(categoriesMDataInfoH, categoriesPermH, categoriesEntriesH); // <----------------------------------------------    Commit ------------------------

                                    var serializedCategoriesMdInfo = await MDataInfo.SerialiseAsync(categoriesMDataInfoH);


                                    // Replace the database stream type info with the updated version
                                    database.Categories = new DataArray { Type = "Buffer", Data = serializedCategoriesMdInfo }; // Points to Md holding stream types

                                    // serialize and encrypt the database
                                    serializedDb = JsonConvert.SerializeObject(database);
                                    dbCipherBytes = await MDataInfo.EncryptEntryValueAsync(appContH, serializedDb.ToUtfBytes());
                                    using (var appContEntryActionsH = await MDataEntryActions.NewAsync())
                                    {
                                        // create the insert action
                                        await MDataEntryActions.InsertAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes);

                                        // Finally update App Container (store db info to it)
                                        await MData.MutateEntriesAsync(appContH, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
                                    }
                                }

                                #endregion Insert new category to Stream Categories Directory MD
                                }
                            }
                        }
                    }
                }
            }
        }

        async Task<Result<bool>> StoreToExistingStream(string databaseId, EventBatch batch)
        {
            // Get the database
            var dstPubIdDigest = await GetMdXorName(databaseId);
            using (var dstPubIdMDataInfoH = await MDataInfo.NewPublicAsync(dstPubIdDigest, 15001))
            {
                using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppSession.AppId)) // appContainerHandle
                {
                    // Deserialize contents
                    var dbIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, databaseId.ToUtfBytes());
                    var (dbCipherBytes, entryVersion) = await MData.GetValueAsync(dstPubIdMDataInfoH, dbIdCipherBytes);
                    var serializedDb = (await MDataInfo.DecryptAsync(appContH, dbCipherBytes)).ToUtfString();
                    var database = JsonConvert.DeserializeObject<Database>(serializedDb);

                    // Get all categories
                    List<(List<byte>, List<byte>, ulong)> dbCategoriesEntries;
                    using (var dbCategoriesDataInfoH = await MDataInfo.DeserialiseAsync(database.Categories.Data))
                    {
                        using (var dbCategoriesDataEntH = await MData.ListEntriesAsync(dbCategoriesDataInfoH))
                        {
                            dbCategoriesEntries = await MDataEntries.ForEachAsync(dbCategoriesDataEntH);
                        }
                    }

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

                    using (var stream_MDataInfoH = await MDataInfo.DeserialiseAsync(streamEntry.Item2))
                    {
                        // First event batch in stream added
                        var jsonBatch = JsonConvert.SerializeObject(batch);
                        var batchKey = $"{batch.Body.First().SequenceNumber}@{batch.Body.Last().SequenceNumber}";

                        using (var streamEntryActionsH = await MDataEntryActions.NewAsync())
                        {
                            // create the insert action
                            await MDataEntryActions.InsertAsync(streamEntryActionsH, batchKey.ToUtfBytes(), jsonBatch.ToUtfBytes());

                            // Finally update md (store batch to it)
                            await MData.MutateEntriesAsync(stream_MDataInfoH, streamEntryActionsH); // <----------------------------------------------    Commit ------------------------
                        }
                    }
                }
            }

            return Result.OK(true);
        }

        #region Helpers

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