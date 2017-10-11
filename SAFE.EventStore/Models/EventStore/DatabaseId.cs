using System;

namespace SAFE.EventStore.Models
{
    public class DatabaseId : IComparable, IEquatable<DatabaseId>
    {
        public string Name { get; }

        public DatabaseId(string name)
        {
            Name = name;
        }

        public int CompareTo(object obj)
        {
            var other = obj as DatabaseId;
            if (other == null)
                throw new NotSupportedException();

            return string.CompareOrdinal(Name, other.Name);
        }

        public bool Equals(DatabaseId other)
        {
            if (ReferenceEquals(null, other))
                return false;

            return ReferenceEquals(this, other) || string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((DatabaseId)obj);
        }

        public override int GetHashCode()
        {
            return Name != null ? Name.GetHashCode() : 0;
        }
    }
}
