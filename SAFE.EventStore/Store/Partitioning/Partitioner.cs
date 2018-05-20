using System;
using System.Collections.Generic;
using System.Text;

namespace SAFE.EventStore.Partitioning
{
    internal static class Partitioner
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value">The value for which we calculate the correct partition, out of the available count of partitions.</param>
        /// <param name="availableCount">Must be a number larger than 0</param>
        /// <returns>Minimum value returned is 0.</returns>
        public static int GetPartition(string value, int availableCount)
        {
            Validate(availableCount);
            ulong hash = GetKey(value);
            return JumpConsistentHash(hash, availableCount);
        }

        public static int GetPartition(Guid value, int availableCount)
        {
            Validate(availableCount);
            ulong hash = GetKey(value);
            return JumpConsistentHash(hash, availableCount);
        }

        public static int GetPartition(byte[] value, int availableCount)
        {
            Validate(availableCount);
            ulong hash = Hash(value);
            return JumpConsistentHash(hash, availableCount);
        }

        public static int GetPartition(List<byte> value, int availableCount)
        {
            Validate(availableCount);
            ulong hash = Hash(value.ToArray());
            return JumpConsistentHash(hash, availableCount);
        }

        static void Validate(int availableCount)
        {
            if (1 > availableCount)
                throw new ArgumentOutOfRangeException("Must have at least one available partition");
        }

        //[Obsolete]
        //int GetShard(Guid guid)
        //{
        //    ulong hash = GetKey(guid);
        //    return JumpConsistentHash(hash, PartitionConstants.Partitions);
        //}

        static ulong GetKey(string value)
        {
            return Hash(Encoding.UTF8.GetBytes(value));
        }

        static ulong GetKey(Guid guid)
        {
            return Hash(guid.ToByteArray());
        }

        static ulong Hash(byte[] bytes)
        {
            return HashFNV1a(bytes);
            //return ToLong(fn1va);
        }

        static int JumpConsistentHash(UInt64 key, Int32 buckets)
        {
            Int64 b = 1;
            Int64 j = 0;

            while (j < buckets)
            {
                b = j;
                key = key * 2862933555777941757 + 1;

                var x = (Double)(b + 1);
                var y = (Double)(((Int64)(1)) << 31);
                var z = (Double)((key >> 33) + 1);

                j = (Int64)(x * (y / z));
            }

            return (Int32)b;
        }

        //[Obsolete]
        //long ToLong(ulong lon)
        //{
        //    if (lon <= long.MaxValue)
        //        return (long)lon;
        //    var diff = lon - long.MaxValue;
        //    return long.MaxValue - (long)diff;
        //}

        // FNV-1a (64-bit) non-cryptographic hash function.
        // Adapted from: http://github.com/jakedouglas/fnv-java
        static ulong HashFNV1a(byte[] bytes)
        {
            const ulong fnv64Offset = 14695981039346656037;
            const ulong fnv64Prime = 0x100000001b3;
            ulong hash = fnv64Offset;

            for (var i = 0; i < bytes.Length; i++)
            {
                hash = hash ^ bytes[i];
                hash *= fnv64Prime;
            }

            return hash;
        }
    }
}
