

using Arch.Core;
using System.Runtime.CompilerServices;

/// <summary>
/// Struct used in inline search to increment year of person by one year.
/// </summary>
public struct AgeUpdate : IForEach<Person>
{
    /// <summary>
    /// Increment age of person by one year.
    /// </summary>
    /// <param name="person">Component containing persons age.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Update(ref Person person)
    {
        person.ageYears++;
    }
}
