using Parquet;
using Parquet.Rows;

internal class Assumptions
{
    internal double[] birth_rate { get; }
    internal double[] female_mortality_rate { get; }
    internal double[] male_mortality_rate { get; }

    /// <summary>
    /// Load assumptions into file
    /// </summary>
    internal Assumptions(string filePath) {
        birth_rate = loadAssumptionToArray(filePath, "Birth Rate - Female");
        female_mortality_rate = loadAssumptionToArray(filePath, "Mortality Rate - Female");
        male_mortality_rate = loadAssumptionToArray(filePath, "Mortality Rate - Male");
    }

    /// <summary>
    /// Asynchronous method to load a parquet file given a file path.
    /// 
    /// For deployment the parquet files can be compiled into the binary and loaded
    /// as a resource stream to remove any dependency on file system layout.
    /// </summary>
    /// <param name="filePath"></param>
    /// <returns></returns>
    internal async Task<Table> loadTableFromParquet(string filePath)
    {
        using FileStream sampleStream = File.OpenRead(filePath);
        using ParquetReader assumptionsReader = await ParquetReader.CreateAsync(sampleStream);
        Table table = await assumptionsReader.ReadAsTableAsync();

        return table;
    }

    /// <summary>
    /// Loads model assumptions from a column witihn a Parquet file.
    /// </summary>
    /// <param name="filePath">Path to the parquet file.</param>
    /// <param name="columnName">Column name in parquet file from which to load assumptions.</param>
    /// <returns>Array of doubles</returns>
    /// <exception cref="Exception">Raises an exception if the file cannot cannot be loaded or the column with the assumptions found.</exception>
    internal double[] loadAssumptionToArray(string filePath, string columnName)
    {
        // Get table from parquet file. Not Parquet.Net is an async library, so use a task runner.
        var task = Task.Run(() => loadTableFromParquet(filePath));
        task.Wait();
        Table table = task.Result;

        // Identify the column reference form the schema in the Parquet file.
        int? columnIndex = null;
        var fields = table.Schema.Fields;
        for (int ix = 0; ix<fields.Count; ix++)
        {
            if (fields[ix].Name == columnName) columnIndex = ix;
        }
        if (columnIndex is null) throw new Exception("Unable to load assumption.");

        double[] assumptionsArray = (from columns in table
                                       select (double)(columns[(int)columnIndex] ?? 0.0)).ToArray();

        return assumptionsArray;
    }

}

