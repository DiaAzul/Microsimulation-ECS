using Arch.CommandBuffer;
using Arch.Core;
using Arch.Core.Utils;
using CommandLine;
using Microsoft.Data.Analysis;
using Synthpop;


public class Options
{
    [Option('p', "population",
        Required = true,
        HelpText = "Population in protobuf file format.")]
    public string population { get; set; }

    [Option('a', "assumptions",
        Required = true,
        HelpText = "Assumptions in Parquet file format.")]
    public string assumptions { get; set; }

    [Option('o', "output",
        Required = true,
        HelpText = "Output to this file in Parquet file format.")]
    public string output { get; set; }
}


public sealed class Microsimulation
{

    public static async Task Main(string[] args)
    {
        // Parse input paths from command line
        var result = Parser.Default.ParseArguments<Options>(args);
        Options options = result.Value;

        string protobufFilePath = options.population;
        string assumptionsFilePath = options.assumptions;
        string outputFilePath = options.output;


        Console.WriteLine("Starting simulation.");
        Random randy = new Random(12000);

        // Load assumptions from file.
        Console.WriteLine("Loading assumptions.");
        var assumptions = new Assumptions(assumptionsFilePath);

        double[] birth_rate = assumptions.birth_rate;
        double[] femaleMortalityRate = assumptions.female_mortality_rate;
        double[] maleMortalityRate = assumptions.male_mortality_rate;

        List<DataFrame> populationPyramids = new List<DataFrame>();
        StringDataFrameColumn rowHeadings = new("Age Bands", [
            "A00-04", "A05-09", "A10-14", "A15-19", "A20-24", "A25-29", "A30-34",
            "A35-39", "A40-44", "A45-49", "A50-54", "A55-59", "A60-64", "A65-69",
            "A70-74", "A75-79", "A80-84", "A85-89", "A90-94", "A95+"]);

        // Read Protobuf file (population assumptions)
        Console.WriteLine("Loading population.");
        Population population;

        using (Stream pb_stream = File.OpenRead(protobufFilePath))
        {
            population = Population.Parser.ParseFrom(pb_stream);
        }

        Console.WriteLine("Creating world.");
        // Create a world and entities
        var world = World.Create();

        var females = from person in population.People
                      where person.Demographics.Sex == Sex.Female
                      select person;
        foreach (var person in females)
        {
            world.Create(new Person
            {
                ageYears = person.Demographics.AgeYears,
                ethnicity = person.Demographics.Ethnicity,
            },
            new IsFemale { },
            new Health
            {
                lifeSatisfaction = person.Health.LifeSatisfaction,
                hasCardiovascularDisease = person.Health.HasCardiovascularDisease,
                hasDiabetes = person.Health.HasDiabetes,
                hasHighBloodPressure = person.Health.HasHighBloodPressure,
                bmi = person.Health.Bmi,
            }
            );
        }

        var males = from person in population.People
                    where person.Demographics.Sex == Sex.Male
                    select person;
        foreach (var person in males)
        {
            world.Create(new Person
            {
                ageYears = person.Demographics.AgeYears,
                ethnicity = person.Demographics.Ethnicity,
            },
            new IsMale { },
            new Health
            {
                lifeSatisfaction = person.Health.LifeSatisfaction,
                hasCardiovascularDisease = person.Health.HasCardiovascularDisease,
                hasDiabetes = person.Health.HasDiabetes,
                hasHighBloodPressure = person.Health.HasHighBloodPressure,
                bmi = person.Health.Bmi,
            }
            );
        }

        var timer = System.Diagnostics.Stopwatch.StartNew();
        Int128 sumOfAges;
        float meanAge = 0;
        var commandBuffer = new CommandBuffer(world, 256);
        var femaleEntity = new ComponentType[] { 
            typeof(Person),
            typeof(IsFemale),
            typeof(Health) 
        };
        var maleEntity = new ComponentType[] {
            typeof(Person),
            typeof(IsMale),
            typeof(Health) 
        };

        Console.WriteLine("Starting simlation");
        // Iterate over twenty years 
        for (uint year = 2021; year < 2041; year++)
        {
            // Increment age by one year.
            world.InlineQuery<AgeUpdate, Person>(in new QueryDescription()
                                                        .WithAll<Person>());

            // Female mortalities and births
            uint femaleMortality = 0;
            uint births = 0;
            var queryFemales = new QueryDescription().WithAll<IsFemale>();
            world.Query(in queryFemales, (Entity entity, ref Person person) =>
            {
                uint ageIndex = Math.Min(person.ageYears, 100);
                double mortalityProbability = femaleMortalityRate[ageIndex];
                if (randy.NextDouble() < mortalityProbability)
                {
                    commandBuffer.Destroy(in entity);
                    femaleMortality++;
                }
                double birthProbability = birth_rate[ageIndex];
                if (randy.NextDouble() < birthProbability)
                {
                    Entity newEntity;
                    births++;
                    if (randy.NextDouble() < 0.5)
                    {
                        newEntity = commandBuffer.Create(femaleEntity);
                        commandBuffer.Set(in newEntity, new IsFemale { });
                    }
                    else
                    {
                        newEntity = commandBuffer.Create(maleEntity);
                        commandBuffer.Set(in newEntity, new IsMale { });
                    }
                    commandBuffer.Set(in newEntity, new Person
                    {
                        // Set childs ethnicity to mother's.
                        ethnicity = person.ethnicity,
                    });
                    commandBuffer.Set(in newEntity, new Health
                    {
                        lifeSatisfaction = LifeSatisfaction.Medium,
                        bmi = 10,
                    });
                }
            });

            // Male mortalities
            uint maleMortality = 0;
            var query_males = new QueryDescription().WithAll<Person, IsMale>();
            world.Query(in query_males, (Entity entity, ref Person person) =>
            {
                uint ageIndex = Math.Min(person.ageYears, 100);
                double mortalityProbability;
                mortalityProbability = maleMortalityRate[ageIndex];
                if (randy.NextDouble() < mortalityProbability)
                {
                    commandBuffer.Destroy(in entity);
                    maleMortality++;
                }
            });

            // Apply structural changes.
            commandBuffer.Playback();

            // Calculate mean age of population
            sumOfAges = 0;
            world.Query(in new QueryDescription().WithAll<Person>(),
                (ref Person person) =>
            {
                sumOfAges += person.ageYears;
            });
            var totalPersons = world.CountEntities(new QueryDescription()
                                                        .WithAll<Person>());
            meanAge = (float)sumOfAges / (float)totalPersons;

            Console.WriteLine("Persons: {0}, Average age: {1}",
                                        totalPersons.ToString(),
                                        meanAge.ToString());
            Console.WriteLine("Births: {0}, Female Deaths: {1}, Male Deaths: {2}",
                                        births,
                                        femaleMortality,
                                        maleMortality);

            // Generate population pyramid for year.
            uint[] yearLabels = new uint[rowHeadings.Count()];
            for (uint i = 0; i < yearLabels.Count(); i++)
            {
                yearLabels[i] = year;
            }

            uint[] female5yrAgePersons = new uint[rowHeadings.Count()];
            world.Query(in new QueryDescription().WithAll<Person, IsFemale>(), (ref Person person) =>
            {
                uint age5yr = Math.Min(19, (uint)Math.Floor(person.ageYears/5.0));
                female5yrAgePersons[age5yr]++;
            });

            uint[] male5yrAgePersons = new uint[rowHeadings.Count()];
            world.Query(in new QueryDescription().WithAll<Person, IsMale>(), (ref Person person) =>
            {
                uint age5yr = Math.Min(19, (uint)Math.Floor(person.ageYears/5.0));
                male5yrAgePersons[age5yr]++;
            });

            PrimitiveDataFrameColumn<uint> yearLabel = new("Year", yearLabels);
            PrimitiveDataFrameColumn<uint> female5yrAge = new("Females", female5yrAgePersons);
            PrimitiveDataFrameColumn<uint> Male5yrAge = new("Males", male5yrAgePersons);
            DataFrame populationPyramid = new(yearLabel, rowHeadings, female5yrAge, Male5yrAge);
            populationPyramids.Add(populationPyramid);
        }

        timer.Stop();
        Console.WriteLine(timer.ElapsedMilliseconds.ToString());

        // Concatenate population pyramids and output to Parquet file.
        DataFrame outputPyramids = populationPyramids.First().Append(
                    from populationPyramid in populationPyramids.Skip(1)
                    from row in populationPyramid.Rows
                    select row
                );
        Export.ToParquet(outputPyramids, outputFilePath);
        Console.WriteLine("End.");
    }
}
