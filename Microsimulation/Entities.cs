
/// <summary>
/// Components containing demogaphic information about a person.
/// For speed sex is held in separate components.
/// </summary>
public struct Person
{
    public uint ageYears;
    public Synthpop.Ethnicity ethnicity;
}

/// <summary>
/// Component containing health imformation about a person.
/// </summary>
public struct Health
{
    public Synthpop.LifeSatisfaction lifeSatisfaction;
    public bool hasCardiovascularDisease;
    public bool hasDiabetes;
    public bool hasHighBloodPressure;
    public float bmi;
}

/// <summary>
/// Component identifying entity as female
/// </summary>
public struct IsFemale { }

/// <summary>
/// Component identifying entity as male.
/// </summary>
public struct IsMale { }
