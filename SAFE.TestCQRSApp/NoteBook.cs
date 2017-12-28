using SAFE.CQRS;
using SAFE.SystemUtils.Events;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAFE.TestCQRSApp
{
    public class NoteBook : Aggregate
    {
        List<string> _notes = new List<string>();

        public async Task<bool> Init(Guid noteBookId)
        {
            await Task.FromResult(0);

            if (_id.HasValue)
                throw new InvalidOperationException("Already initiated");
            RaiseEvent(new NoteBookInitiated(noteBookId));
            return true;
        }

        public async Task<bool> AddNote(string note)
        {
            await Task.FromResult(0);

            if (_notes.Contains(note))
                return false;  // i.e. nothing changed

            var validOperation = note != null && note.Length < 100;

            if (!validOperation) // protect invariants
                throw new InvalidOperationException("Cannot be null and cannot exceed 100 chars.");
            
            RaiseEvent(new NoteAdded(this.StreamId, note)); // applies the change to current state

            return validOperation; // returning true means state changed
        }

        void Apply(NoteBookInitiated e)
        {
            _id = e.NotebookId;
        }

        void Apply(NoteAdded e)
        {
            _notes.Add(e.Note);
        }
    }

    public class NoteBookInitiated : Event
    {
        public NoteBookInitiated(Guid notebookId)
        {
            NotebookId = notebookId;
        }

        public Guid NotebookId { get; private set; }
    }

    public class NoteAdded : Event
    {
        public NoteAdded(Guid notebookId, string note)
        {
            NotebookId = notebookId;
            Note = note;
        }

        public Guid NotebookId { get; private set; }
        public string Note { get; private set; }
    }
}