using System.Globalization;
using System.Net;

namespace AsyncResponse.Channels.Redis;

/// <summary>
/// The slice of a <c>CLUSTER NODES</c> reply the recovery scan needs: which address each node is
/// reachable at, and whether it owns slots as a primary. Parsed from the raw reply — the text
/// format is Redis's own wire contract — rather than read through the client's
/// <c>ClusterConfiguration</c>, which cannot be constructed outside the client and would leave
/// this classification untestable.
/// <para>
/// One line per node: <c>id ip:port@cport[,hostname] flags master-id ping pong epoch link-state
/// [slot ...]</c>. Redis 3 omits <c>@cport</c>; an IPv6 address carries colons of its own, so the
/// port is whatever follows the LAST colon.
/// </para>
/// </summary>
internal static class RedisClusterNodeTable
{
    internal readonly record struct Node(string Address, string? HostName, int Port, bool IsReplica, bool HasSlots);

    private const int FirstSlotField = 8;

    internal static List<Node> Parse(string nodeTable)
    {
        var nodes = new List<Node>();
        foreach (var line in nodeTable.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var fields = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (fields.Length < 3)
                continue;

            var address = fields[1];
            string? hostName = null;
            if (address.IndexOf(',') is var comma and >= 0)
            {
                hostName = address[(comma + 1)..];
                address = address[..comma];
            }

            if (address.IndexOf('@') is var bus and >= 0)
                address = address[..bus];

            var portSeparator = address.LastIndexOf(':');
            if (portSeparator <= 0
                || !int.TryParse(address.AsSpan(portSeparator + 1), NumberStyles.None, CultureInfo.InvariantCulture, out var port))
                continue;

            var flags = fields[2].Split(',');
            nodes.Add(new Node(
                address[..portSeparator],
                string.IsNullOrEmpty(hostName) ? null : hostName,
                port,
                IsReplica: Array.Exists(flags, static flag => flag is "slave" or "replica"),
                HasSlots: fields.Length > FirstSlotField));
        }

        return nodes;
    }

    /// <summary>
    /// <c>true</c> only when the table LISTS <paramref name="endPoint"/> and lists it as a replica
    /// or as a node without slots. An endpoint the table does not list (a DNS name the cluster
    /// does not announce, a stale configuration entry) is unknown, and unknown is not "owns
    /// nothing".
    /// </summary>
    internal static bool OwnsNoSlots(List<Node> nodes, EndPoint? endPoint)
    {
        var (host, port) = endPoint switch
        {
            IPEndPoint ip => (ip.Address.ToString(), ip.Port),
            DnsEndPoint dns => (dns.Host, dns.Port),
            _ => (null, 0)
        };
        if (host is null)
            return false;

        foreach (var node in nodes)
        {
            if (node.Port != port || !(SameHost(node.Address, host) || (node.HostName is { } announced && SameHost(announced, host))))
                continue;

            return node.IsReplica || !node.HasSlots;
        }

        return false;
    }

    private static bool SameHost(string left, string right)
        => IPAddress.TryParse(left, out var leftAddress) && IPAddress.TryParse(right, out var rightAddress)
            ? leftAddress.Equals(rightAddress)
            : string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
}
