//---------------------------------------------------------------------------------- 
// Copyright (c) Microsoft Corporation. All rights reserved. 
// 
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,  
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES  
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE. 
//---------------------------------------------------------------------------------- 
// The example companies, organizations, products, domain names, 
// e-mail addresses, logos, people, places, and events depicted 
// herein are fictitious.  No association with any real company, 
// organization, product, domain name, email address, logo, person, 
// places, or events is intended or should be inferred. 

using Benchmark;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Windows.Forms;
using System.Windows.Forms.DataVisualization.Charting;
using System.Windows.Forms.Integration;


namespace DemoWorkload
{
    public partial class FrmMain : Form
    {

        /// <summary> 
        /// Executes Write/Read Commands
        /// </summary> 
        void OnRunClick(object sender, EventArgs e)
        {
            try
            {
                lock (StopLock)
                {
                    Stopped = false;
                }

                string ReadCommand;
                string WriteCommand;

                WriteCommand = "EXEC BatchInsertReservations @ServerTransactions, @RowsPerTransaction, @ThreadID";
                ReadCommand = "EXEC ReadMultipleReservations @ServerTransactions, @RowsPerTransaction, @ThreadID";
                this.ErrorMessages.Clear();

                ThreadParams tp = new ThreadParams(Program.REQUEST_COUNT, Program.TRANSACTION_COUNT, Program.ROW_COUNT,
                    Program.READS_PER_WRITE, ReadCommand, WriteCommand);
                
                //for (int j = 0; j < Program.THREAD_COUNT; j++)
                //{
                //    int Threads = RunningThreads.Count();
                //    // Create a thread with parameters.
                //    ParameterizedThreadStart pts = new ParameterizedThreadStart(ThreadWorker);
                //    RunningThreads.Add(new Thread(pts));
                //    RunningThreads.ElementAt(Threads).Start(tp);
                //}

                Runner runner = new Runner();
                runner.Init(tp);
                runner.Run(tp);
                runner.Dispose();

                // Thread Monitor
                ThreadStart ts1 = new ThreadStart(ThreadMonitor);
                this.MonitorThread = new Thread(ts1);
                this.MonitorThread.Start();
            }
            catch (Exception ex) { ShowThreadExceptionDialog("OnRunClick", ex); }
        }

        /// <summary> 
        /// Executes Transactions on the target server
        /// </summary>
        void ThreadWorker(object tp)
        {
            ////////////////////////////////////////////////////////////////////////////////
            // Connect to the data source.
            ////////////////////////////////////////////////////////////////////////////////

            System.Data.SqlClient.SqlConnection conn = new SqlConnection(Program.CONN_STR);

            ThreadParams MyTP = (ThreadParams)tp;
            SqlCommand WriteCmd = new SqlCommand();
            WriteCmd.Connection = conn;
            WriteCmd.CommandTimeout = 600;
            WriteCmd.CommandText = MyTP.WriteCommandText;
            WriteCmd.Parameters.Add("@ServerTransactions", SqlDbType.Int, 4).Value = (int)MyTP.serverTransactions;
            WriteCmd.Parameters.Add("@RowsPerTransaction", SqlDbType.Int, 4).Value = (int)MyTP.rowsPerTransaction;
            WriteCmd.Parameters.Add("@ThreadID", SqlDbType.Int, 4).Value = (int)Thread.CurrentThread.ManagedThreadId;

            SqlCommand ReadCmd = new SqlCommand();
            ReadCmd.Connection = conn;
            ReadCmd.CommandTimeout = 600;
            ReadCmd.CommandText = MyTP.ReadCommandText;
            ReadCmd.Parameters.Add("@ServerTransactions", SqlDbType.Int, 4).Value = (int)MyTP.serverTransactions;
            ReadCmd.Parameters.Add("@RowsPerTransaction", SqlDbType.Int, 4).Value = (int)MyTP.rowsPerTransaction;
            ReadCmd.Parameters.Add("@ThreadID", SqlDbType.Int, 4).Value = (int)Thread.CurrentThread.ManagedThreadId;

            // Executing transactions on the target server
            try
            {
                conn.Open();
                for (int i = 0; i < MyTP.requestsPerThread; i++)
                {
                    lock (StopLock)
                    {
                        if (Stopped)
                        {
                            break;
                        }
                    }
                    WriteCmd.ExecuteNonQuery();
                    for (int j = 0; j < MyTP.readsPerWrite && !Stopped; j++)
                    {
                        ReadCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                lock (this.ErrorLock)
                {
                    this.AddText(ex.Message + " " + Thread.CurrentThread.ManagedThreadId.ToString());
                }
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary> 
        /// Thread Monitor
        /// </summary>
        void ThreadMonitor()
        {
            //Set-up & initialization
            DateTime Start = DateTime.Now;
            DateTime PerfCounterStart = DateTime.Now, PerfCounterEnd;
            Int64 LastPerfCounterValue = 0, ThisPerfCounterValue = 0, TPS = 0;
            Int64 LatchCounterValue = 0, LastLatchCounter = 0, ThisLatchCounter = 0;
            int CPU_Usage = 0;
            int mo_tables = 0;
            PerformanceCounter CPUCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total");
            Int64 TotalTPS = 0, TotalIterations = 0;

            // Open the connection
            System.Data.SqlClient.SqlConnection conn = new SqlConnection(Program.CONN_STR);

            string cmdStr = string.Format("select max(cntr_value) FROM sys.dm_os_performance_counters WHERE counter_name = 'Transactions/sec'");

            string LatchcmdStr = string.Format("select max(cntr_value) FROM sys.dm_os_performance_counters WHERE counter_name = 'Latch Waits/sec'");
            SqlCommand Perfcmd = new SqlCommand();
            Perfcmd.Connection = conn;
            Perfcmd.CommandTimeout = 600;
            Perfcmd.CommandText = cmdStr;

            //query to determine which stage we're in
            SqlCommand Latchcmd = new SqlCommand();
            Latchcmd.Connection = conn;
            Latchcmd.CommandTimeout = 600;
            Latchcmd.CommandText = LatchcmdStr;
            string ConfigSelect = string.Format("select count(*) from sys.tables where is_memory_optimized = 1 and object_id=object_id('dbo.TicketReservationDetail')");

            SqlCommand ConfigQuery = new SqlCommand();
            ConfigQuery.Connection = conn;
            ConfigQuery.CommandTimeout = 600;
            ConfigQuery.CommandText = ConfigSelect;

            try
            {
                conn.Open();
            }
            catch (Exception ex)
            {
                lock (this.ErrorLock)
                {
                    this.AddText(ex.Message + " " + Thread.CurrentThread.ManagedThreadId.ToString());
                }
            }

            if (conn.State != ConnectionState.Open)
            {
                MessageBox.Show("Monitor failed to connect to server.", "Error", MessageBoxButtons.OK);
                return;
            }

            mo_tables = (int)ConfigQuery.ExecuteScalar();
            if (mo_tables == 0)
            {
                //This is the case where there are no Memory Optimized tables, so we're running in pure SQL mode.
                UpdateResults("Baseline");
                //Calling UpdateTPSChart with a negative value causes it to clear the current chart and reset.
                UpdateTPSChart(-1);
            }
            else
            {
                UpdateResults("");
            }

            while (RunningThreads.Count > 0)
            {
                List<Thread> DeadThreads = new List<Thread>();
                foreach (Thread MyThread in RunningThreads)
                {
                    if (!MyThread.IsAlive)
                    {
                        DeadThreads.Add(MyThread);
                    }
                }
                foreach (Thread DThread in DeadThreads)
                {
                    RunningThreads.Remove(DThread);
                }
                DeadThreads.Clear();
                PerfCounterEnd = DateTime.Now;

                try
                {
                    ThisPerfCounterValue = (Int64)Perfcmd.ExecuteScalar();
                    ThisLatchCounter = (Int64)Latchcmd.ExecuteScalar();
                }
                catch (Exception ex)
                {
                    lock (this.ErrorLock)
                    {
                        this.AddText(ex.Message + " " + Thread.CurrentThread.ManagedThreadId.ToString());
                    }
                }

                if (LastLatchCounter == 0)
                {
                    LastLatchCounter = ThisLatchCounter;
                }

                if (LastPerfCounterValue != 0)
                {
                    CPU_Usage = (int)CPUCounter.NextValue();

                    TimeSpan PerfCounterInterval = PerfCounterEnd - PerfCounterStart;
                    if (PerfCounterInterval.Milliseconds > 0)
                    {
                        TPS = (Int64)((ThisPerfCounterValue - LastPerfCounterValue) / (float)(PerfCounterInterval.Seconds + (PerfCounterInterval.Milliseconds / 1000)));
                        LatchCounterValue = (Int64)((ThisLatchCounter - LastLatchCounter) / (float)(PerfCounterInterval.Seconds + (PerfCounterInterval.Milliseconds / 1000)));
                        UpdateCPUChart(CPU_Usage);
                        UpdateLatchChart(LatchCounterValue);
                        UpdateTPSChart(TPS);
                    }
                    if (mo_tables == 0)
                    {
                        TotalTPS += TPS;
                        TotalIterations += 1;
                        BaselineTPS = TotalTPS / TotalIterations;
                    }
                    else
                    {
                        string UpdateString = string.Format("{0}X", TPS / BaselineTPS);
                        UpdateResults(UpdateString);
                    }
                }

                PerfCounterStart = PerfCounterEnd;
                LastPerfCounterValue = ThisPerfCounterValue;
                LastLatchCounter = ThisLatchCounter;
                PerfCounterStart = PerfCounterEnd;
                this.UpdateCount(RunningThreads.Count.ToString());
                Thread.Sleep(1000);
                TimeSpan Elapsed = DateTime.Now - Start;
                UpdateElapsed(Elapsed.ToString(@"hh\:mm\:ss"));
            }

            TPS = 0;
            CPU_Usage = 0;
            LatchCounterValue = 0;
            UpdateLatchChart(LatchCounterValue);
            UpdateCPUChart(CPU_Usage);
            UpdateTPSChart(TPS);
        }

    }


    /// <summary> 
    /// ThreadParams Class
    /// </summary>
    class ThreadParams
    {
        public int requestsPerThread;  // how many many separate client requests per thread
        public int serverTransactions; // how many separate transactions to run on the server per request
        public int rowsPerTransaction; // how many rows to inserts/read per transaction
        public int readsPerWrite;      // number of read requests per write request
        public string ReadCommandText; // command text for read request
        public string WriteCommandText; // command text for insert request

        public ThreadParams(int requestsPerThread, int serverTransactions, int rowsPerTransaction, int readsPerWrite,
            string ReadCommandText, string WriteCommandText)
        {
            this.requestsPerThread = requestsPerThread;
            this.serverTransactions = serverTransactions;
            this.rowsPerTransaction = rowsPerTransaction;
            this.readsPerWrite = readsPerWrite;
            this.ReadCommandText = ReadCommandText;
            this.WriteCommandText = WriteCommandText;
        }

    }

}