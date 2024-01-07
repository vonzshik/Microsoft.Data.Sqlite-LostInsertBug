using System.Data;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

const string ConnectionString = "Data Source=mydb.db";
const int Rows = 1_000;
const int Threads = 4;
const int Step = Rows / Threads;

CreateTable();

var threads = new Task[Threads];

for (var i = 0; i < threads.Length; i++)
{
    var begin = Step * i;
    var end = begin + Step;
    threads[i] = Task.Run(() => InsertRows_LostUpdate(begin, end));
}

await Task.WhenAll(threads);

AssertRowCount();

return 0;

static void CreateTable()
{
    using var conn = new SqliteConnection(ConnectionString);
    conn.Open();

    using var cmd = conn.CreateCommand();
    cmd.CommandText = "DROP TABLE IF EXISTS my_table";
    cmd.ExecuteNonQuery();

    cmd.CommandText = """
                      CREATE TABLE my_table(
                      a INTEGER PRIMARY KEY,
                      b DATE DEFAULT CURRENT_TIMESTAMP,
                      c INTEGER
                      );
                      """;
    cmd.ExecuteNonQuery();
}

static void InsertRows_LostUpdate(int begin, int end)
{
    using var conn = new SqliteConnection(ConnectionString);
    conn.Open();

    using var cmd = conn.CreateCommand();

    cmd.CommandText = "SELECT 1; INSERT INTO my_table VALUES(@a, @b, @c) RETURNING *;";
    var paramA = cmd.Parameters.Add(new SqliteParameter("@a", SqliteType.Integer));
    var paramB = cmd.Parameters.Add(new SqliteParameter("@b", DateTime.UtcNow));
    var paramC = cmd.Parameters.Add(new SqliteParameter("@c", SqliteType.Integer));

    for (var i = begin; i < end; i++)
    {
        paramA.Value = i;
        paramB.Value = DateTime.UtcNow;
        paramC.Value = i % 42;

        cmd.ExecuteNonQuery();
    }
}

static void AssertRowCount()
{
    using var conn = new SqliteConnection(ConnectionString);
    conn.Open();

    using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT COUNT(*) FROM my_table";
    var rowCount = (long)cmd.ExecuteScalar();
    if (rowCount != Rows)
    {
        throw new Exception($"Unexpected row count: {rowCount} instead of {Rows}");
    }
}
