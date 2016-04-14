using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Threading;

namespace Benchmark
{
    interface IRunner
    {
    }

    class Runner : IRunner, IDisposable
    {
        SqlConnection _conn;
        SqlCommand _writeCmd;
        SqlCommand _readCmd;

        public bool Stopped = false;

        public void Init(object load)
        {
            var connectionString = DemoWorkload.Program.CONN_STR;
                        
            var conn = new SqlConnection(connectionString);

            var MyTP = (DemoWorkload.ThreadParams)load;
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

            conn.Open();

            this._conn = conn;
            this._writeCmd = WriteCmd;
            this._readCmd = ReadCmd;
        }

        public void Run(object load)
        {
            var MyTP = (DemoWorkload.ThreadParams)load;
            var WriteCmd = this._writeCmd;
            var ReadCmd = this._readCmd;

            for (int i = 0; i < MyTP.requestsPerThread; i++)
            {
                WriteCmd.ExecuteNonQuery();
                for (int j = 0; j < MyTP.readsPerWrite; j++)
                {
                    ReadCmd.ExecuteNonQuery();
                }
            }
        }

        public void Dispose()
        {
            if(_conn != null)
            {
                _conn.Dispose();
            }
        }
    }
}
