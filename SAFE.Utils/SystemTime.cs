using System;

namespace SAFE.SystemUtils
{
    /// <summary>
    /// Used for getting DateTime.UtcNow, time is changeable for unit testing
    /// </summary>
    public static class SystemTime
    {
        static Func<DateTime> _utcNow = () => DateTime.UtcNow;
        static Func<DateTime> _now = () => DateTime.Now;
        /// <summary> Normally this is a pass-through to DateTime.UtcNow, but it can be overridden with SetDateTime( .. ) for testing or debugging.
        /// </summary>
        public static DateTime UtcNow
        {
            get { return _utcNow(); }
        }

        public static DateTime Now
        {
            get { return _now(); }
        }

        /// <summary> Set time to return when SystemTime.UtcNow is called.
        /// </summary>
        public static void SetDateTime(DateTime dateTimeUtcNow)
        {
            _utcNow = () => dateTimeUtcNow;
            _now = () => dateTimeUtcNow.ToLocalTime(); // Todo: Test that this is actually same as DateTime.Now!
        }

        /// <summary> Resets SystemTime.UtcNow to return DateTime.UtcNow.
        /// </summary>
        public static void ResetDateTime()
        {
            _utcNow = () => DateTime.UtcNow;
        }
    }
}