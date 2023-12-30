using Microsoft.Data.Analysis;
using Parquet;


public class Export
{
    static public async void ToParquet(DataFrame dataFrame, string filePath)
    {
        using FileStream fileStream = File.OpenWrite(filePath);
        await dataFrame.WriteAsync(fileStream);
    }
}

