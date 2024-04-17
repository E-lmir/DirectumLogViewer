using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Renci.SshNet;

namespace SshConfigParser
{
  public class SshHost
  {

    public SftpClient? sftpClient;

    /// <summary>
    /// Identity file for the host.
    /// </summary>
    public string IdentityFile
    {
      get => this[nameof(IdentityFile)]?.ToString();
      set => this[nameof(IdentityFile)] = value;
    }

    /// <summary>
    /// The host alias
    /// </summary>
    public string Host
    {
      get => this[nameof(Host)]?.ToString();
      set => this[nameof(Host)] = value;
    }

    /// <summary>
    /// Full host name
    /// </summary>
    public string HostName
    {
      get => this[nameof(HostName)]?.ToString();
      set => this[nameof(HostName)] = value;
    }

    /// <summary>
    /// User
    /// </summary>
    public string User
    {
      get => this[nameof(User)]?.ToString();
      set => this[nameof(User)] = value;
    }

    /// <summary>
    /// Port
    /// </summary>
    public string Port
    {
      get => this[nameof(Port)]?.ToString();
      set => this[nameof(Port)] = value;
    }

    /// <summary>
    /// A collection of all the properties for this host, including those explicitly defined.
    /// </summary>
    public Dictionary<string, object> Properties { get; set;  } = new Dictionary<string, object>();

    public object this[string key]
    {
      get
      {
        if (Properties.TryGetValue(key, out var value))
          return value;

        return null;
      }
      set { Properties[key] = value; }
    }

    /// <summary>
    /// Keys of all items in the SSH host.
    /// </summary>
    public IEnumerable<string> Keys => Properties.Keys;
  }
}
