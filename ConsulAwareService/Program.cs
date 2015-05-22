using System;
using System.Configuration;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Consul;
using Microsoft.Owin.Hosting;
using Microsoft.Win32.SafeHandles;

namespace OWINSelfHostSampleConsole
{
    class Program : IDisposable
    {
        bool _disposed;
        // Instantiate a SafeHandle instance.
        readonly SafeHandle _handle = new SafeFileHandle(IntPtr.Zero, true);
        internal Client ConsulClient;
        Consul.Semaphore _semaphore;
        private const int ListenPort = 9000;
        private const string ListenAddress = "+";
        private readonly bool _consulEnabled;

        public Program()
        {
            _disposed = false;
            if (ConfigurationManager.AppSettings["consul"] == "true")
            {
                _consulEnabled = CreateConsulClient();
                Console.WriteLine("Joined consul: {0}", _consulEnabled);
            }
        }

        bool CreateConsulClient()
        {
            ConsulClient = new Client();
            //Deregister any leftover service
            ConsulClient.Agent.ServiceDeregister("AmazingService").Wait(5000);
            AgentServiceRegistration svcreg = new AgentServiceRegistration
            {
                Name = "AmazingService",
                Port = ListenPort,
                Address = ListenAddress,
                //,
                //        Tags = new[] { "consultest", "owintest" },
                Check = new AgentServiceCheck
                {
                    TTL = TimeSpan.FromSeconds(5)
                }
            };
            //if (_consulClient.Agent.ServiceRegister(svcreg).Wait(5000)) _semaphore = _consulClient.Semaphore("AmazingService/lock", 1);
            return ConsulClient.Agent.ServiceRegister(svcreg).Wait(5000);
        }
       

        internal class XService : IDisposable
        {
            private readonly Program _parent;
            private const string Name = "XService";
            private const int Limit = 1;
            private const int RestartInterval = 5000;
            private const string Prefix = Name + "/lock";

            private bool _disposed;
            public bool Running
            {
                get { return (_actionThread != null && _actionThread.IsAlive); }
            }

            private Thread _actionThread;
            private CancellationTokenSource _actionCancellationTokenSource;
            private EventHandler ActionCancelEventHandler { get; set; }
            private Thread _monitorThread;
            private CancellationTokenSource _monitorCancellationTokenSource;
            private EventHandler MonitorCancelEventHandler { get; set; }
            private Thread _getLockThread;
            private CancellationTokenSource _getLockCancellationTokenSource;
            private EventHandler GetLockCancelEventHandler { get; set; }

            private SemaphoreOptions _semaphoreOptions = new SemaphoreOptions(Prefix, Limit) { SessionName = Name + "_Session", SessionTTL = TimeSpan.FromSeconds(10)};
            private Consul.Semaphore _semaphore;

            internal XService(Program parent)
            {
                _parent = parent;
                if (_parent._consulEnabled)
                {
                    if (RegisterSvcInConsul())
                    {
                        RegisterInternalMonitorCheck();
                        RegisterThreadMonitorCheck();
                        _monitorThread = MonitorThread();
                        _monitorThread.Start();
                                                
                    }
                }                
            }
            internal void Start()
            {
                if (_parent._consulEnabled)
                {
                    _getLockThread = GetLockThread();
                    _getLockThread.Start();
                }
                else
                {
                    _actionThread = ActionThread();
                    _actionThread.Start();
                }                
            }
            internal bool Stop(int timeout = 30000)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                if (ActionCancelEventHandler != null) ActionCancelEventHandler.Invoke(this, new EventArgs());
                if (GetLockCancelEventHandler != null) GetLockCancelEventHandler.Invoke(this, new EventArgs());
                while (sw.ElapsedMilliseconds < timeout)
                {
                    bool actiondone = ((_actionThread == null) || (!_actionThread.IsAlive));
                    bool getlockdone = ((_getLockThread == null) || (!_getLockThread.IsAlive));
                    if (actiondone && getlockdone) break;
                }
                if (_semaphore != null && _semaphore.IsHeld) _semaphore.Release();
                try
                {
                    if (_semaphore != null) _semaphore.Destroy();
                }
                catch (SemaphoreInUseException) { }
                return true;
            }

            internal bool RegisterSvcInConsul()
            {
                _parent.ConsulClient.Agent.ServiceDeregister(Name).Wait(5000);
                AgentServiceRegistration svcreg = new AgentServiceRegistration
                {
                    Name = Name
                };
                return _parent.ConsulClient.Agent.ServiceRegister(svcreg).Wait(5000);
            }
            internal bool RegisterThreadMonitorCheck()
            {
                AgentCheckRegistration cr = new AgentCheckRegistration
                {
                    Name = Name + "_Threadcheck",
                    TTL = TimeSpan.FromSeconds(5),
                    Notes = "Checks if service thread is alive",
                    ServiceID = Name
                };
                return _parent.ConsulClient.Agent.CheckRegister(cr).Wait(5000);
            }
            internal bool RegisterInternalMonitorCheck()
            {
                AgentCheckRegistration cr = new AgentCheckRegistration
                {
                    Name = Name + "_Internalcheck",
                    TTL = TimeSpan.FromSeconds(5),
                    Notes = "Status from within service thread",
                    ServiceID = Name
                };
                return _parent.ConsulClient.Agent.CheckRegister(cr).Wait(5000);
            }

            internal Thread MonitorThread()
            {
                _monitorCancellationTokenSource = new CancellationTokenSource();
                MonitorCancelEventHandler += (sender, obj) => { _monitorCancellationTokenSource.Cancel(); };
                return new Thread(Monitor);
            }
            internal Thread ActionThread()
            {
                _actionCancellationTokenSource = new CancellationTokenSource();
                ActionCancelEventHandler += (sender, obj) => { _actionCancellationTokenSource.Cancel(); };
                return new Thread(Actions);
            }
            internal Thread GetLockThread()
            {
                _getLockCancellationTokenSource = new CancellationTokenSource();
                GetLockCancelEventHandler += (sender, obj) => { _getLockCancellationTokenSource.Cancel(); };                
                return new Thread(GetLock);
            }

            private void Monitor()
            {
                Console.WriteLine("Running monitor thread: {0}", _monitorThread.ManagedThreadId);
                while (!(_monitorCancellationTokenSource.IsCancellationRequested))
                {
                    if (_actionThread != null && _actionThread.IsAlive)
                    {
                        _parent.ConsulClient.Agent.PassTTL(Name + "_Threadcheck", "Alive").Wait(1000);
                    }
                    else
                    {
                        _parent.ConsulClient.Agent.FailTTL(Name + "_Threadcheck", "Not Alive").Wait(1000);
                    }
                    Thread.Sleep(1000);
                }
                Console.WriteLine("Stopping my subclass service");
            }
            private void Actions()
            {
                Console.WriteLine("Running my subclass service: {0}", _actionThread.ManagedThreadId);
                while ((!(_parent._consulEnabled) || (_parent._consulEnabled && _semaphore.IsHeld))&&(!(_actionCancellationTokenSource.IsCancellationRequested)))
                //while (!(_actionCancellationTokenSource.IsCancellationRequested))
                {
                    Console.Write("X");
                    _parent.ConsulClient.Agent.PassTTL(Name + "_Internalcheck", "Alive").Wait(1000);
                    Thread.Sleep(1000);
                }
                if (_actionCancellationTokenSource.IsCancellationRequested)
                {
                    Console.WriteLine("Cancel of actionthread requested, stopping thread");
                }
                else
                {
                    Console.WriteLine("Lock on semaphore lost, stopping action thread");
                    Thread.Sleep(RestartInterval);
                    _getLockThread = GetLockThread();
                    _getLockThread.Start();
                }
            }
            private void GetLock()
            {
                Console.WriteLine("Trying to acquire lock");
                if (_semaphore != null) try { _semaphore.Destroy(); }
                    catch (SemaphoreInUseException) { }
                _semaphoreOptions = new SemaphoreOptions(Prefix, Limit) { SessionName = Name + "_Session", SessionTTL = TimeSpan.FromSeconds(10) };
                _semaphore = _parent.ConsulClient.Semaphore(_semaphoreOptions);
                _semaphore.Acquire(_getLockCancellationTokenSource.Token);
                if (_getLockCancellationTokenSource.IsCancellationRequested)
                {
                    Console.WriteLine("Cancelling Lock aqcuisition");
                }
                else
                {
                    Console.WriteLine("Lock acquired, becoming active");
                    _actionThread = ActionThread();
                    _actionThread.Start();
                }
                
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }
            protected virtual void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    if (disposing)
                    {
                        // Free other state (managed objects).
                    }
                    // Free your own state (unmanaged objects).
                    // Set large fields to null.
                    if (MonitorCancelEventHandler != null)
                    {
                        Console.WriteLine("Stopping monitor thread");
                        MonitorCancelEventHandler.Invoke(this, new EventArgs());
                    }
                    if (_parent._consulEnabled) _parent.ConsulClient.Agent.ServiceDeregister(Name).Wait(5000);
                    _disposed = true;
                }
            }
            ~XService()
            {
                Dispose(false);
            }
        }

        

        static void Main()
        {
            Program prog = new Program();
            var svc = new XService(prog);
            svc.Start();



            Console.WriteLine("Press 'c' to cancel this service.");
            while (Console.ReadKey(true).KeyChar != 'c') Thread.Sleep(100);

            Console.WriteLine("Stopping all threads");
            bool stopped = svc.Stop();
            if (stopped)
            {
                svc.Dispose();
            }
            

            Console.WriteLine("All done, Press enter to exit.");
            Console.ReadLine();
        }

        public void Dispose()
        {
            // Dispose of unmanaged resources.
            Dispose(true);
            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                _handle.Dispose();
                // Free any other managed objects here. 
                //
            }
            // Free any unmanaged objects here. 
            //
            //if (_semaphore != null && _semaphore.IsHeld) _semaphore.Release();
            //if (_semaphore != null) try { _semaphore.Destroy(); }
            //    catch (SemaphoreInUseException) { }
            //if (_consulEnabled) ConsulClient.Agent.ServiceDeregister().Wait(5000);
            _disposed = true;
        }
    }
}
