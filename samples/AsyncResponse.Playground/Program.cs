using System.Diagnostics;

var repositoryRoot = FindRepositoryRoot(Directory.GetCurrentDirectory());
if (repositoryRoot is null)
{
    Console.Error.WriteLine("Could not find AsyncResponse.sln from the current working directory.");
    return 1;
}

var appHostProject = Path.Combine(
    repositoryRoot.FullName,
    "samples",
    "AsyncResponse.AppHost",
    "AsyncResponse.AppHost.csproj");

if (!File.Exists(appHostProject))
{
    Console.Error.WriteLine($"Could not find Aspire AppHost project at '{appHostProject}'.");
    return 1;
}

var dotnet = ResolveDotNetExecutable();
var dotnetDirectory = Path.GetDirectoryName(dotnet);

var startInfo = new ProcessStartInfo
{
    FileName = dotnet,
    WorkingDirectory = repositoryRoot.FullName,
    UseShellExecute = false
};

startInfo.ArgumentList.Add("run");
startInfo.ArgumentList.Add("--project");
startInfo.ArgumentList.Add(appHostProject);
startInfo.ArgumentList.Add("--launch-profile");
startInfo.ArgumentList.Add("http");

startInfo.Environment["ASPNETCORE_ENVIRONMENT"] = "Development";
startInfo.Environment["DOTNET_ENVIRONMENT"] = "Development";
startInfo.Environment["ASPIRE_ALLOW_UNSECURED_TRANSPORT"] = "true";

if (!string.IsNullOrWhiteSpace(dotnetDirectory))
{
    startInfo.Environment["PATH"] = PrependPath(
        dotnetDirectory,
        startInfo.Environment.TryGetValue("PATH", out var path) ? path : null);
}

using var appHost = Process.Start(startInfo);
if (appHost is null)
{
    Console.Error.WriteLine("Could not start Aspire AppHost.");
    return 1;
}

var stopping = false;

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stopping = true;
    StopAppHost(appHost);
};

AppDomain.CurrentDomain.ProcessExit += (_, _) => StopAppHost(appHost);

await appHost.WaitForExitAsync();
return stopping ? 0 : appHost.ExitCode;

static DirectoryInfo? FindRepositoryRoot(string startDirectory)
{
    for (var directory = new DirectoryInfo(startDirectory);
         directory is not null;
         directory = directory.Parent)
    {
        if (File.Exists(Path.Combine(directory.FullName, "AsyncResponse.sln")))
        {
            return directory;
        }
    }

    return null;
}

static string ResolveDotNetExecutable()
{
    const string macOSDefaultDotNet = "/usr/local/share/dotnet/dotnet";

    if (File.Exists(macOSDefaultDotNet))
    {
        return macOSDefaultDotNet;
    }

    return "dotnet";
}

static string PrependPath(string directory, string? existingPath)
{
    if (string.IsNullOrWhiteSpace(existingPath))
    {
        return directory;
    }

    var paths = existingPath.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);
    return paths.Contains(directory, StringComparer.Ordinal)
        ? existingPath
        : directory + Path.PathSeparator + existingPath;
}

static void StopAppHost(Process appHost)
{
    try
    {
        if (!appHost.HasExited)
        {
            appHost.Kill(entireProcessTree: true);
        }
    }
    catch (InvalidOperationException)
    {
    }
}
