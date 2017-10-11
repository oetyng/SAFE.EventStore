
namespace SAFE.SystemUtils
{
    public class Result<T>
    {
        public bool OK { get; private set; }
        public bool Error { get; private set; }
        public string ErrorMsg { get; }

        public T Value { get; private set; }

        internal Result(T value)
        {
            Value = value;
            OK = true;
            ErrorMsg = string.Empty;
        }
        internal Result(string errorMsg, bool error)
        {
            Error = true;
            ErrorMsg = errorMsg ?? string.Empty;
        }
    }

    public static class Result
    {
        public static Result<T> OK<T>(T value)
        {
            return new Result<T>(value);
        }

        public static Result<T> Fail<T>(string errorMsg)
        {
            return new Result<T>(errorMsg, true);
        }
    }
}