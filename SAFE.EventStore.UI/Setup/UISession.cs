using SAFE.EventStore.Services;
using System;

namespace SAFE.EventStore.UI.Setup
{
    public class UISession
    {
        bool _mock;

        IAppSession _sessionInstance;
        IEventStoreService _storeInstance;

        public UISession(bool mock)
        {
            _mock = mock;
        }


        public bool Authenticated
        {
            get => _sessionInstance != null && _sessionInstance.IsAuthenticated;
        }


        public IEventStoreService EventStoreService()
        {
            if (!Authenticated)
                throw new InvalidOperationException("Not authenticated.");

            if (_storeInstance != null)
                return _storeInstance;

            if (_mock)
                _storeInstance = new MockEventStoreService();
            else
                _storeInstance = new EventStoreService();

            return _storeInstance;
        }

        public IAppSession AppSession()
        {
            if (_sessionInstance != null)
                return _sessionInstance;

            if (_mock)
                _sessionInstance = new MockAppSession();
            else
                _sessionInstance = new AppSession();

            return _sessionInstance;
        }

        internal void Reset()
        {
            _sessionInstance?.Dispose();
            _storeInstance?.Dispose();
            _sessionInstance = null;
            _storeInstance = null;
        }
    }

    public class LogoutService
    {
        UISession _session;

        public LogoutService(UISession session)
        {
            _session = session;
        }

        public void Logout()
        {
            _session.Reset();
        }
    }
}