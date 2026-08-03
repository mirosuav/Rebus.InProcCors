namespace Rebus.InProcCors.Sample;

/// <summary>
/// A timestamped, module-tagged console writer. Each module gets its own colour so the interleaving
/// of handler output is readable - and the interleaving is real: Rebus dispatches to handlers on
/// worker threads, so two modules handling the same published event genuinely run concurrently.
/// </summary>
sealed class ConsoleLog(string module, ConsoleColor color)
{
    // One process-wide gate. Console.ForegroundColor is global mutable state, so without this the
    // colour set by one module's line can bleed onto another module's line.
    static readonly Lock Gate = new();

    const int Width = 76;

    /// <summary>Writes one timestamped, module-tagged line in this module's colour.</summary>
    public void Write(string message) => Raw($"{DateTime.Now:HH:mm:ss.fff}  [{module,-9}] {message}", color);

    /// <summary>Writes a centred section heading. Used by the scenario driver to separate runs.</summary>
    public static void Banner(string text)
    {
        var padding = Math.Max(0, (Width - text.Length - 2) / 2);
        Raw("", ConsoleColor.Gray);
        Raw($"{new string('=', padding)} {text} {new string('=', padding)}", ConsoleColor.White);
    }

    /// <summary>Writes an undecorated line - used for the header and the colour legend.</summary>
    public static void Note(string text, ConsoleColor color = ConsoleColor.DarkGray) => Raw(text, color);

    static void Raw(string text, ConsoleColor color)
    {
        lock (Gate)
        {
            var previous = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(text);
            Console.ForegroundColor = previous;
        }
    }
}
