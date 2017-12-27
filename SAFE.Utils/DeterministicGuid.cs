using System;
using System.Text;

namespace SAFE.Utils
{
    /// <summary>
    /// https://gist.github.com/dnauck/6190ac506b05691cc29d
    /// </summary>
    public static class DeterministicGuid
    {
        /// <summary>
        /// Creates and returns a deterministic <see cref="Guid"/> from the input string.
        /// This extension method could be used to build a <see cref="Guid"/> from a natural 
        /// or external key of any entity.
        /// </summary>
        /// <param name="value">The input string.</param>
        /// <returns>A deterministic <see cref="Guid"/>.</returns>
        public static Guid ToDeterministicGuid(this string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return Guid.Empty;
            var md5Digest = new Org.BouncyCastle.Crypto.Digests.MD5Digest();
            var md5ResultBuffer = new byte[md5Digest.GetDigestSize()];

            var input = Encoding.UTF8.GetBytes(value);
            md5Digest.BlockUpdate(input, 0, input.Length);
            md5Digest.DoFinal(md5ResultBuffer, 0);

            return new Guid(md5ResultBuffer);
        }
    }
}