using System;
using System.Windows.Input;
using SAFE.DotNET.Models;

namespace SAFE.DotNET
{
    // TODO, fix proper
    internal class Command : ICommand
    {
        private Action _action;

        public Command(Action action)
        {
            _action = action;
        }

        public event EventHandler CanExecuteChanged;

        public bool CanExecute(object parameter)
        {
            return _action != null;
        }

        public void Execute(object parameter)
        {
            _action();
        }
    }

    internal class Command<T> : ICommand
    {
        private Action _action;
        private Action<UserId> _paramAction1;
        private Action<Message> _paramAction2;

        public Command(Action action)
        {
            _action = action;
        }

        public Command(Action<UserId> action)
        {
            _paramAction1 = action;
        }

        public Command(Action<Message> action)
        {
            _paramAction2 = action;
        }

        public event EventHandler CanExecuteChanged;

        public bool CanExecute(object parameter)
        {
            return _action != null;
        }

        public void Execute(object parameter)
        {
            _action();
        }
    }
}