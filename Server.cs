using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace IRCServer
{
    public class Server
    {
        public class Config
        {
            public bool HideIPs = true;
            public bool EnableAuthServ = true;
            public bool LogToFile = true;
            public bool LogChannelMessages = true;
            public bool LogDirectMessages = true;
            public string LogFileEOL = "\r\n";
            public string MOTD = "Welcome to an unnamed IRC server. Admins can change this message in config.json.";
            public string AutoJoinChannel = null;
        }

        public class UserSentMessageEventArgs
        {
            public Channel ToChannel;
            public User ToUser;
            public string Message;
            public bool Cancel;
        }
        public class Profile
        {
            public List<string> AutoJoinChannels = new List<string>();
        }

        internal static string NICKNAME = $"....NICKNAME{Guid.NewGuid().ToString()}....";
        static TcpListener listener;
        public static List<User> Users;
        public static List<Channel> Channels;
        static Dictionary<string, string> passwords;

        public static event Action<User> UserConnected;
        public static event Action<User> UserDisconnected;
        public static event Action<User, Channel> UserJoinedChannel;
        public static event Action<User, Channel, string> UserPartedChannel;
        public static event Action<User, UserSentMessageEventArgs> UserSentMessage;
        public static event Action<Channel, char, string, User> ChannelModeAdded;
        public static event Action<Channel, char, string, User> ChannelModeRemoved;
        public static event Action<User, string, string[]> UserSentData;

        public static readonly Random Random = new Random();

        static Dictionary<string, Profile> profiles; 

        public static Config Configuration;

        public static void Log(string line)
        {
            line = $"[{DateTime.Now}] {line}";
            Console.WriteLine(line);
            if (Configuration.LogToFile)
            {
                if (!File.Exists("./IRCServer.log")) File.WriteAllText("./IRCServer.log", "");
                File.AppendAllText("./IRCServer.log", line + Configuration.LogFileEOL);
            }
        }

        static void Main(string[] args)
        {
            listener = new TcpListener(IPAddress.Any, 6667);
            listener.Start();

            Users = new List<User>();
            Channels = new List<Channel>();

            if (File.Exists("./passwd.json"))
            {
                passwords = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText("./passwd.json"));
            }
            else
            {
                passwords = new Dictionary<string, string>();
            }

            if (File.Exists("./config.json"))
            {
                Configuration = JsonConvert.DeserializeObject<Config>(File.ReadAllText("./config.json"));
            }
            else
            {
                Configuration = new Config();
                File.WriteAllText("./config.json", JsonConvert.SerializeObject(Configuration, Formatting.Indented));
            }

            if (File.Exists("./profile.json"))
            {
                profiles = JsonConvert.DeserializeObject<Dictionary<string, Profile>>(File.ReadAllText("./profile.json"));
            }
            else
            {
                profiles = new Dictionary<string, Profile>();
                File.WriteAllText("./profile.json", JsonConvert.SerializeObject(profiles, Formatting.Indented));
            }

            if (File.Exists("./chan.json"))
            {
                Channels = JsonConvert.DeserializeObject<List<Channel>>(File.ReadAllText("./chan.json")).Select((ch) =>
                {
                    if (!(ch.Users is List<User>)) ch.Users = new List<User>();
                    return ch;
                }).ToList();
            }
            else
            {
                Channels = new List<Channel>();
            }

            if (Directory.Exists("./plugins"))
            {
                foreach (var pluginPath in Directory.GetFiles("./plugins"))
                {
                    if (!pluginPath.EndsWith(".dll")) continue;
                    try
                    {
                        var plugin = Assembly.LoadFrom(pluginPath);
                        var mainType = plugin.GetType($"{plugin.GetName().Name}.Plugin", true, false);
                        var main = Activator.CreateInstance(mainType) as dynamic;
                        main.Started();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Can't load {pluginPath}: {ex.ToString()}");
                    }
                }
            }

            UserConnected += Server_UserConnected;

            listener.BeginAcceptTcpClient(AcceptClient, listener);

            new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        foreach (var user in Users)
                        {
                            user.Flush();
                        }
                    } catch (Exception)
                    {

                    }
                }
            }).Start();

            new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(10000);
                    try
                    {
                        foreach (var user in Users)
                        {
                            user.Send($"PING amogus");
                        }
                    }
                    catch (Exception)
                    {

                    }
                }
            }).Start();

            while (true)
            {

            }
        }

        private static void Server_UserConnected(User sender)
        {
            if (profiles.ContainsKey(sender.Nickname))
            {
                foreach (var channelName in profiles[sender.Nickname].AutoJoinChannels)
                {
                    User_MessageReceived(sender, $"JOIN {channelName}");
                }
            }
            if (Configuration.AutoJoinChannel is string)
            {
                var channel = FindChannel(Configuration.AutoJoinChannel);
                if (channel is Channel && !channel.Users.Contains(sender))
                {
                    User_MessageReceived(sender, $"JOIN {channel.Name}");
                }
            }
        }

        public static void SavePasswords()
        {
            File.WriteAllText("./passwd.json", JsonConvert.SerializeObject(passwords));
        }

        public static void SaveChannels()
        {
            File.WriteAllText("./chan.json", JsonConvert.SerializeObject(Channels));
        }

        public static void SaveProfiles()
        {
            File.WriteAllText("./profile.json", JsonConvert.SerializeObject(profiles));
        }

        public static User FindUser(string nickname)
        {
            var f = Users.Find((x) => x.Nickname == nickname);
            return f == default(User) ? null : f;
        }

        public static Channel FindChannel(string name)
        {
            var f = Channels.Find((x) => x.Name == name);
            return f == default(Channel) ? null : f;
        }

        static void AcceptClient(IAsyncResult ar)
        {
            var user = new User(listener.EndAcceptTcpClient(ar));
            Users.Add(user);
            user.Quit += User_Quit;
            user.MessageReceived += User_MessageReceived;
            listener.BeginAcceptTcpClient(AcceptClient, listener);
        }

        private static void User_MessageReceived(User sender, string line)
        {
            if (line == null || line.Length < 1) return; // ignore
            //Console.WriteLine(line);
            var arr = line.Split(' ');
            var args = arr.Skip(1).Where(x => !string.IsNullOrEmpty(x)).ToArray();
            var cmd = arr[0];

            if (cmd == "PASS" && !(sender.Username is string))
            {
                if (args.Length >= 1)
                {
                    var pass = string.Join(" ", args);
                    if (pass.StartsWith(":")) pass = pass.Substring(1);
                    sender.Pass = pass;
                }
                return;
            }

            if (cmd == "PONG")
            {
                if (args.Length < 1) return;
                var pongCode = args[0];
                if (pongCode.StartsWith(":")) pongCode = pongCode.Substring(1);
                sender.Ponged = true;
            }

            if (cmd == "NICK")
            {
                if (sender.Nickname is string) return;
                args[0] = args[0].Trim();
                if (args.Length == 0)
                {
                    sender.Send(":Serv 431 No nickname given");
                    return;
                }
                if (Users.Where(x => x.Nickname is string && x.Nickname.ToLower() == args[0].ToLower()).Count() > 0)
                {
                    sender.Send($":Serv 433 {sender.Nickname} {args[0]} :Nickname is already in use");
                    return;
                }
                sender.Nickname = args[0];
                sender.Send($":{sender.Prefix()} NICK {sender.Nickname}");
            }
            else if (cmd == "USER")
            {
                if (sender.Username is string && sender.Hostname is string && sender.Servername is string &&
                        sender.Realname is string) return;
                if (args.Length < 4)
                {
                    sender.Send(":Serv 461 USER :Not enough parameters");
                    return;
                }
                sender.Username = args[0];
                sender.Hostname = args[1];
                sender.Servername = args[2];
                sender.Realname = string.Join(' ', args.Skip(3));

                if (sender.Realname.StartsWith(":")) sender.Realname = sender.Realname.Substring(1);

                if (!(sender.Nickname is string))
                {
                    sender.Nickname = sender.Username;
                    sender.Send($":{sender.Prefix()} NICK {sender.Nickname}");
                }

                sender.Send($":Serv 001 {sender.Nickname} : Start of MOTD");

                foreach (var motdline in Configuration.MOTD.Split("\n"))
                {
                    sender.Send($":Serv 300 {sender.Nickname} : {motdline}");
                }

                sender.Send($":Serv 002 {sender.Nickname} : End of MOTD");

                sender.Send($":Serv MODE {sender.Nickname} +w");
                Log($"{sender.Prefix(false)} connected");
                if (Configuration.EnableAuthServ && sender.Pass is string)
                {
                    var pwd = sender.Pass;
                    if (passwords.ContainsKey(sender.Nickname))
                    {
                        if (passwords[sender.Nickname] != pwd)
                        {
                            sender.Send($":AuthServ 300 {sender.Nickname} :Passwords didn't match!");
                        }
                        else
                        {
                            sender.Send($":AuthServ 300 {sender.Nickname} :You have authorized successfully!");
                            sender.Authorized = true;
                            UserConnected?.Invoke(sender);
                        }
                        return;
                    }
                    if (pwd.Length < 3 || pwd.Length > 20)
                    {
                        sender.Send($":AuthServ 300 {sender.Nickname} :Your password is too short or long! Please choose another.");
                        sender.Close();
                    }
                    else
                    {
                        passwords.Add(sender.Nickname, pwd);
                        SavePasswords();
                        sender.Send($":AuthServ 300 {sender.Nickname} :Nickname successfully registered!");
                        sender.Authorized = true;
                        UserConnected?.Invoke(sender);
                    }
                    return;
                }
                
                if (!Configuration.EnableAuthServ)
                    UserConnected?.Invoke(sender);
            }
            if (Configuration.EnableAuthServ)
            {
                if (!sender.Authorized)
                {
                    
                    if (cmd == "PASS")
                    {
                        if (args.Length < 1)
                        {
                            sender.Send(":Serv 461 PASS :Not enough parameters");
                            return;
                        }
                        var pwd = string.Join(" ", args);
                        if (passwords.ContainsKey(sender.Nickname))
                        {
                            if (passwords[sender.Nickname] != pwd)
                            {
                                sender.Send($":AuthServ 300 {sender.Nickname} :Passwords didn't match!");
                            }
                            else
                            {
                                sender.Send($":AuthServ 300 {sender.Nickname} :You have authorized successfully!");
                                sender.Authorized = true;
                                UserConnected?.Invoke(sender);
                            }
                            return;
                        }
                        if (pwd.Length < 3 || pwd.Length > 20)
                        {
                            sender.Send($":AuthServ 300 {sender.Nickname} :Your password is too short or long! Please choose another.");
                        }
                        else
                        {
                            passwords.Add(sender.Nickname, pwd);
                            SavePasswords();
                            sender.Send($":AuthServ 300 {sender.Nickname} :Nickname successfully registered!");
                            sender.Authorized = true;
                            UserConnected?.Invoke(sender);
                        }
                        return;
                    }
                    if (cmd == "PRIVMSG")
                    {
                        if (args.Length < 2)
                        {
                            sender.Send(":Serv 461 PRIVMSG :Not enough parameters");
                            return;
                        }
                        var channelName = args[0];
                        var message = string.Join(" ", args.Skip(sender.IsRevolution ? 3 : 1).ToArray());
                        if (message.StartsWith(":")) message = message.Substring(1);
                        var arg_s = message.Split(" ");
                        if (channelName == "AuthServ")
                        {
                            if (arg_s.Length == 1 && arg_s[0] == "help")
                            {
                                sender.Send($":AuthServ NOTICE {sender.Nickname} :AuthServ allows you protect your nickname");
                                sender.Send($":AuthServ NOTICE {sender.Nickname} :And prevent others from using it.");
                                sender.Send($":AuthServ NOTICE {sender.Nickname} :/MSG AuthServ register <password> - protect your nickname with a password");
                                sender.Send($":AuthServ NOTICE {sender.Nickname} :/MSG AuthServ auth <password> - authorize your connection");
                                sender.Send($":AuthServ NOTICE {sender.Nickname} :/PASS <password> - register if your nickname isn't protected, otherwise authorizes connection");
                            }
                            else if (arg_s.Length == 2 && arg_s[0] == "register")
                            {
                                var pwd = arg_s[1];
                                if (passwords.ContainsKey(sender.Nickname))
                                {
                                    sender.Send($":AuthServ NOTICE {sender.Nickname} :Your nickname is already registered! If you forgot your password,");
                                    sender.Send($":AuthServ NOTICE {sender.Nickname} :please reconnect with another nickname OR contact admins!");
                                    return;
                                }
                                if (pwd.Length < 3 || pwd.Length > 20)
                                {
                                    sender.Send($":AuthServ NOTICE {sender.Nickname} :Your password is too short or long! Please choose another.");
                                }
                                else
                                {
                                    passwords.Add(sender.Nickname, pwd);
                                    SavePasswords();
                                    sender.Send($":AuthServ NOTICE {sender.Nickname} :Nickname successfully registered!");
                                    sender.Authorized = true;
                                    UserConnected?.Invoke(sender);
                                }
                            }
                            else if (arg_s.Length == 2 && arg_s[0] == "auth")
                            {
                                var pwd = arg_s[1];
                                if (!passwords.ContainsKey(sender.Nickname))
                                {
                                    sender.Send($":AuthServ NOTICE {sender.Nickname} :Your nickname is not registered! Register with \"/MSG AuthServ register <password>\"");
                                    return;
                                }
                                if (passwords[sender.Nickname] != pwd)
                                {
                                    sender.Send($":AuthServ NOTICE {sender.Nickname} :Passwords didn't match!");
                                }
                                else
                                {
                                    sender.Send($":AuthServ NOTICE {sender.Nickname} :You have authorized successfully!");
                                    sender.Authorized = true;
                                    UserConnected?.Invoke(sender);
                                }
                            }
                            return;
                        }

                    }
                    sender.Send($":AuthServ NOTICE {sender.Nickname} :You are not authorized! Type \"/MSG AuthServ help\" to get help for authorization.");
                    return;
                }
            } else
            {
                sender.Authorized = true;
            }

            if (sender.Nickname == null || sender.Username == null || sender.Realname == null) return;

            UserSentData?.Invoke(sender, cmd, args);

            if (cmd == "JOIN")
            {
                if (args.Length < 1)
                {
                    sender.Send(":Serv 461 JOIN :Not enough parameters");
                    return;
                }
                foreach (var channelName in args[0].Split(','))
                {
                    var channel = FindChannel(channelName);
                    if (channel is Channel)
                    {
                        if ((channel.HasFlag('p') || channel.HasFlag('i')) && !channel.HasMode('i', sender))
                        {
                            sender.Send($":Serv 473 {sender.Nickname} : You're not invited to {channelName}");
                            return;
                        }
                        if (channel.HasMode('b', sender))
                        {
                            sender.Send($":Serv 474 {sender.Nickname} : You're banned from {channelName}");
                            return;
                        }
                        if (channel.Users.Contains(sender)) return;
                        channel.Join(sender);
                        if (!profiles.ContainsKey(sender.Nickname)) profiles.Add(sender.Nickname, new Profile());
                        if (!profiles[sender.Nickname].AutoJoinChannels.Contains(channelName)) profiles[sender.Nickname].AutoJoinChannels.Add(channelName);
                        SaveProfiles();
                        Log($"{sender.Prefix(false)} joined {channelName}");
                    }
                    else
                    {
                        try
                        {
                            channel = new Channel(channelName);
                            if (!channel.HasValidName())
                            {
                                sender.Send(":Serv 461 JOIN :Bad paramater");
                                return;
                            }
                            channel.Join(sender);
                            channel.AddMode('o', sender);
                            Channels.Add(channel);
                            Log($"{sender.Prefix(false)} created and joined {channelName}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }
                    }
                }
                SaveChannels();
            }
            else if (cmd == "PRIVMSG")
            {
                if (args.Length < 2)
                {
                    sender.Send(":Serv 461 PRIVMSG :Not enough parameters");
                    return;
                }
                var channelName = args[0];
                var originalMessage = string.Join(" ", args.Skip(sender.IsRevolution ? 3 : 1).ToArray());
                if (originalMessage.StartsWith(":")) originalMessage = originalMessage.Substring(1);
                var message = originalMessage;
                message = message.Replace("[c]", "\x03");
                foreach (System.Text.RegularExpressions.Match match in System.Text.RegularExpressions.Regex.Matches(message, @"\[c:(\d\d?(,\d\d?)?)\]"))
                {
                    try
                    {
                        var g = match.Groups.Values.ToArray()[1].Value;
                        message = message.Replace(match.Value, "\x03" + g);
                    }
                    catch (Exception)
                    {

                    }
                }
                if (channelName.StartsWith("#") || channelName.StartsWith("&"))
                {
                    var channel = FindChannel(channelName);
                    if (channel is Channel)
                    {
                        if (!channel.Users.Contains(sender))
                        {
                            sender.Send($":Serv 442 {sender.Nickname} {channelName} :You're not on that channel");
                            return;
                        }
                        if ((channel.HasFlag('m') && !channel.HasMode('o', sender) && !channel.HasMode('v', sender)) || channel.HasMode('q', sender))
                        {
                            sender.Notice(channelName, "You don't have permission to send messages here");
                        }
                        else
                        {
                            var ev = new UserSentMessageEventArgs()
                            {
                                Cancel = false,
                                Message = message,
                                ToChannel = channel,
                                ToUser = null
                            };
                            UserSentMessage?.Invoke(sender, ev);
                            if (ev.Cancel) return;
                            if (message == originalMessage)
                                channel.BroadcastMessage(sender, message);
                            else
                                channel.SendMessage(sender, message);
                            if (Configuration.LogChannelMessages) Log($"{sender.Prefix(false)} -> {channelName} :: {originalMessage}");
                        }
                    }
                    else
                    {
                        sender.Send($":Serv 403 PRIVMSG {channelName} :No such channel");
                    }
                }
                else
                {
                    var user = FindUser(channelName);
                    if (user is User)
                    {
                        var ev = new UserSentMessageEventArgs()
                        {
                            Cancel = false,
                            Message = message,
                            ToChannel = null,
                            ToUser = user
                        };
                        UserSentMessage?.Invoke(sender, ev);
                        if (ev.Cancel) return;
                        if (user.IsRevolution)
                        {
                            user.Send($":{sender.Prefix()} PRIVMSG {channelName} {sender.Nickname} :{sender.Username} :{message}");
                        }
                        else
                        {
                            user.Send($":{sender.Prefix()} PRIVMSG {channelName} :{message}");
                        }
                        if (message != originalMessage)
                            sender.Send($":{sender.Prefix()} PRIVMSG {channelName} :{message}");
                        if (Configuration.LogDirectMessages) Log($"{sender.Prefix(false)} -> {channelName} :: {originalMessage}");
                    }
                    else
                    {
                        sender.Send($":Serv 403 PRIVMSG {channelName} :No such channel");
                    }
                }
            }
            else if (cmd == "NOTICE")
            {
                if (args.Length < 2)
                {
                    sender.Send(":Serv 461 NOTICE :Not enough parameters");
                    return;
                }
                var channelName = args[0];
                var message = string.Join(" ", args.Skip(sender.IsRevolution ? 3 : 1).ToArray());
                if (message.StartsWith(":")) message = message.Substring(1);
                if (channelName.StartsWith("#") || channelName.StartsWith("&"))
                {
                    var channel = FindChannel(channelName);
                    if (channel is Channel)
                    {
                        if (!channel.Users.Contains(sender))
                        {
                            sender.Send($":Serv 442 {sender.Nickname} {channelName} :You're not on that channel");
                            return;
                        }
                        if ((channel.HasFlag('m') && !channel.HasMode('o', sender) && !channel.HasMode('v', sender)) || channel.HasMode('q', sender))
                        {
                            sender.Notice(channelName, "You don't have permission to send messages here");
                        }
                        else
                        {
                            channel.BroadcastNotice(sender, message);
                            if (Configuration.LogChannelMessages) Log($"[!] {sender.Prefix(false)} -> {channelName} :: {message}");
                        }
                    }
                    else
                    {
                        sender.Send($":Serv 403 NOTICE {channelName} :No such channel");
                    }
                }
                else
                {
                    var user = FindUser(channelName);
                    if (user is User)
                    {
                        if (user.IsRevolution)
                        {
                            user.Send($":{sender.Prefix()} NOTICE {channelName} {sender.Nickname} :{sender.Username} :{message}");
                        }
                        else
                        {
                            user.Send($":{sender.Prefix()} NOTICE {channelName} :{message}");
                        }
                        if (Configuration.LogDirectMessages) Log($"[!] {sender.Prefix(false)} -> {channelName} :: {message}");
                    }
                    else
                    {
                        sender.Send($":Serv 403 NOTICE {channelName} :No such channel");
                    }
                }
            }
            else if (cmd == "TOPIC")
            {
                if (args.Length < 2)
                {
                    sender.Send(":Serv 461 TOPIC :Not enough parameters");
                    return;
                }
                var channelName = args[0];
                var message = string.Join(" ", args.Skip(sender.IsRevolution ? 3 : 1).ToArray());
                if (message.StartsWith(":")) message = message.Substring(1);
                if (channelName.StartsWith("#") || channelName.StartsWith("&"))
                {
                    var channel = FindChannel(channelName);
                    if (channel is Channel)
                    {
                        if (!channel.HasMode('o', sender))
                        {
                            sender.Send($":Serv 482 {sender.Nickname} {channelName} :You're not channel operator");
                        }
                        else
                        {
                            channel.SetTopic(message);
                            Log($"{sender.Prefix(false)} changes topic for {channelName} :: {message}");
                        }
                    }
                    else
                    {
                        sender.Send($":Serv 403 TOPIC {channelName} :No such channel");
                    }
                }
                else
                {
                    sender.Send($":Serv 403 TOPIC {channelName} :No such channel");
                }
            }
            else if (cmd == "QUIT")
            {
                sender.Close();
            }
            else if (cmd == "PING")
            {
                if (args.Length == 1)
                {
                    sender.Send($"PONG {args[0]}");
                } else
                {
                    sender.Send("PONG");
                }
            }
            else if (cmd == "MODE")
            {
                
                if (args.Length < 1)
                {
                    sender.Send(":Serv 461 MODE :Not enough parameters");
                    return;
                }
                var channelName = args[0];
                var channel = FindChannel(channelName);
                if (channel is Channel)
                {
                    if (!channel.Users.Contains(sender))
                    {
                        sender.Send($":Serv 442 {sender.Nickname} {channelName} :You're not on that channel");
                        return;
                    }
                    if (args.Length < 2)
                    {
                        sender.Send($":Serv 324 {sender.Nickname} {channelName} {string.Join(" ", channel.Flags)}");
                        return;
                    }
                    if (!channel.HasMode('o', sender))
                    {
                        sender.Send($":Serv 482 {sender.Nickname} {channelName} :You're not channel operator");
                        return;
                    }
                    else
                    {
                        var action = args[1][0] == '+';
                        var modes = args[1].Substring(1);
                        if (args.Length >= 3)
                        {
                            var target = args[2];
                            foreach (var mode in modes)
                            {
                                if (action)
                                    channel.AddMode(mode, target, sender);
                                else
                                    channel.RemoveMode(mode, target, sender);
                            }
                            if (modes.Contains('b') && action)
                            {
                                var targetUser = FindUser(target);
                                if (targetUser is User && channel.Users.Contains(targetUser))
                                {
                                    channel.Part(targetUser, "Banned");
                                    targetUser.Send($":Serv NOTICE {targetUser.Nickname} :You're banned from {channelName}");
                                    channel.RemoveMode('i', target, sender);
                                }
                            }
                        }
                        else
                        {
                            foreach (var mode in modes)
                            {
                                if (action)
                                    channel.AddFlag(mode, sender);
                                else
                                    channel.RemoveFlag(mode, sender);
                            }
                        }
                        SaveChannels();
                    }
                }
                else
                {
                    sender.Send($":Serv 403 MODE {channelName} :No such channel");
                }
            }
            else if (cmd == "PART")
            {
                if (args.Length < 1)
                {
                    sender.Send(":Serv 461 PART :Not enough parameters");
                    return;
                }
                var channelName = args[0];
                var channel = FindChannel(channelName);
                if (channel is Channel)
                {
                    if (!channel.Users.Contains(sender))
                    {
                        sender.Send($":Serv 442 {sender.Nickname} {channelName} :You're not on that channel");
                        return;
                    }
                    // гг я ливаю
                    channel.Part(sender, "Parting");
                    Log($"{sender.Prefix(false)} parts channel {channelName}");

                    if (profiles.ContainsKey(sender.Nickname))
                    {
                        profiles[sender.Nickname].AutoJoinChannels.Remove(channelName);
                        SaveProfiles();
                    }
                }
                else
                {
                    sender.Send($":Serv 403 PART {channelName} :No such channel");
                }
            }
            else if (cmd == "KICK")
            {
                if (args.Length < 2)
                {
                    sender.Send(":Serv 461 KICK :Not enough parameters");
                    return;
                }
                var channelName = args[0];
                var channel = FindChannel(channelName);
                var target = FindUser(args[1]);
                var reason = "Not set";
                if (args.Length > 2) reason = string.Join(" ", args.Skip(2));
                if (reason.StartsWith(":")) reason = reason.Substring(1);
                if (channel is Channel)
                {
                    if (!channel.Users.Contains(sender))
                    {
                        sender.Send($":Serv 442 {sender.Nickname} {channelName} :You're not on that channel");
                        return;
                    }
                    if (!channel.Users.Contains(target) || !(target is User))
                    {
                        sender.Send($":Serv 442 {sender.Nickname} {channelName} :No such user");
                        return;
                    }
                    if (!channel.HasMode('o', sender))
                    {
                        sender.Send($":Serv 482 {sender.Nickname} {channelName} :You're not channel operator");
                        return;
                    }

                    channel.Part(target, $"Kicked by {sender.Prefix()}: {reason}");

                    target.Send($":Serv NOTICE {sender.Nickname} :You were kicked from {channelName}. Reason: {reason}");
                    Log($"{sender.Prefix(false)} kicked {target.Prefix(false)} from {channelName}");
                }
                else
                {
                    sender.Send($":Serv 403 KICK {channelName} :No such channel");
                }
            }
            else
            {
            }
        }

        private static void User_Quit(User sender)
        {
            //Console.WriteLine($"{sender.Nickname ?? "NoNickName"} disconnected");

            foreach (var channel in Channels.Where(x => x.Users.Contains(sender)))
            {
                channel.Part(sender, "Disconnected");
            }

            Log($"{sender.Prefix(false)} disconnected");

            Users.Remove(sender);
        }

        public class Channel
        {
            public List<char> Flags;
            public Dictionary<string, List<char>> Modes;
            [JsonIgnore]
            public List<User> Users;
            public string Name;
            public string Topic;

            public Channel(string Name, string Topic = "")
            {

                Flags = new List<char>();
                Modes = new Dictionary<string, List<char>>();
                Users = new List<User>();

                this.Name = Name;
                this.Topic = Topic;

            }

            public bool HasValidName()
            {
                if (Name.Length < 2 || Name.Length > 20) return false;
                if (Name[0] != '#' && Name[0] != '&') return false;
                if (Name.Contains(" ") || Name.Contains(",") || Name.Contains("\x07")) return false;
                return true;
            }

            public void SetTopic(string topic)
            {
                Topic = topic;
                Send($":Serv {(Topic.Length == 0 ? 331 : 332)} {Server.NICKNAME} {Name} :{(Topic.Length == 0 ? "No topic is set." : Topic)}");
            }

            public void Join(User user)
            {
                Users.Add(user);
                Send($":{user.Prefix()} JOIN {Name} {user.Username ?? user.Nickname} :{user.Realname}");
                user.Send($":Serv 353 {user.Nickname} {(HasMode('o', user) ? "@" : HasMode('v', user) ? "+" : "=")} {Name} {string.Join(" ", Users.Select(user => (HasMode('o', user) ? "@" : HasMode('v', user) ? "+" : "") + user.Nickname))}");
                user.Send($":Serv 366 {user.Nickname} {Name} :End of /NAMES list.");
                user.Send($":Serv {(Topic.Length == 0 ? 331 : 332)} {user.Nickname} {Name} :{(Topic.Length == 0 ? "No topic is set." : Topic)}");
                UserJoinedChannel?.Invoke(user, this);

                foreach (var mode in Flags)
                {
                    Send($":Serv MODE {Name} +{mode}");
                }

                if (Modes.ContainsKey(user.Nickname))
                foreach (var mode in Modes[user.Nickname])
                {
                    Send($":Serv MODE {Name} +{mode} {user.Nickname}");
                }
            }

            public void Part(User user, string reason = null)
            {
                // if (Modes.ContainsKey(user.Nickname)) Modes.Remove(user.Nickname);

                Send($":{user.Prefix()} PART {Name}" + (reason is string ? (" :" + reason) : ""));

                Users.Remove(user);
                UserPartedChannel?.Invoke(user, this, reason);
            }

            public bool HasFlag(char flag)
            {
                return Flags.Contains(flag);
            }

            public void AddFlag(char flag, User whoSets = null)
            {
                if (!Flags.Contains(flag))
                    Flags.Add(flag);
                Send($":{(whoSets is User ? whoSets.Prefix() : "Serv")} MODE {Name} +{flag}");
                ChannelModeAdded?.Invoke(this, flag, null, whoSets);
                Log($"{(whoSets is User ? whoSets.Prefix(false) : "Serv")} sets +{flag} on {Name}");
            }

            public void RemoveFlag(char flag, User whoSets = null)
            {
                if (Flags.Contains(flag))
                    Flags.Remove(flag);
                Send($":{(whoSets is User ? whoSets.Prefix() : "Serv")} MODE {Name} -{flag}");
                ChannelModeRemoved?.Invoke(this, flag, null, whoSets);
                Log($"{(whoSets is User ? whoSets.Prefix(false) : "Serv")} sets -{flag} on {Name}");
            }

            public bool HasMode(char flag, User user)
            {
                return Modes.ContainsKey(user.Nickname) && Modes[user.Nickname].Contains(flag);
            }

            public void AddMode(char flag, User user, User whoSets = null)
            {
                if (!Modes.ContainsKey(user.Nickname)) Modes.Add(user.Nickname, new List<char>());
                if (!Modes[user.Nickname].Contains(flag)) Modes[user.Nickname].Add(flag);
                Send($":{(whoSets is User ? whoSets.Prefix() : "Serv")} MODE {Name} +{flag} {user.Nickname}");

                ChannelModeAdded?.Invoke(this, flag, user.Nickname, whoSets);
                Log($"{(whoSets is User ? whoSets.Prefix(false) : "Serv")} sets +{flag} on {Name}, {user.Prefix(false)}");
            }

            public void RemoveMode(char flag, User user, User whoSets = null)
            {
                if (!Modes.ContainsKey(user.Nickname)) Modes.Add(user.Nickname, new List<char>());
                if (Modes[user.Nickname].Contains(flag)) Modes[user.Nickname].Remove(flag);
                Send($":{(whoSets is User ? whoSets.Prefix() : "Serv")} MODE {Name} -{flag} {user.Nickname}");

                ChannelModeRemoved?.Invoke(this, flag, user.Nickname, whoSets);
                Log($"{(whoSets is User ? whoSets.Prefix(false) : "Serv")} sets -{flag} on {Name}, {user.Prefix(false)}");
            }

            public bool HasMode(char flag, string user)
            {
                return Modes.ContainsKey(user) && Modes[user].Contains(flag);
            }

            public void AddMode(char flag, string user, User whoSets = null)
            {
                if (!Modes.ContainsKey(user)) Modes.Add(user, new List<char>());
                if (!Modes[user].Contains(flag)) Modes[user].Add(flag);
                Send($":{(whoSets is User ? whoSets.Prefix() : "Serv")} MODE {Name} +{flag} {user}");
                ChannelModeAdded?.Invoke(this, flag, user, whoSets);
                Log($"{(whoSets is User ? whoSets.Prefix(false) : "Serv")} sets +{flag} on {Name}, {user}");
            }

            public void RemoveMode(char flag, string user, User whoSets = null)
            {
                if (!Modes.ContainsKey(user)) Modes.Add(user, new List<char>());
                if (Modes[user].Contains(flag)) Modes[user].Remove(flag);
                Send($":{(whoSets is User ? whoSets.Prefix() : "Serv")} MODE {Name} -{flag} {user}");
                ChannelModeRemoved?.Invoke(this, flag, user, whoSets);
                Log($"{(whoSets is User ? whoSets.Prefix(false) : "Serv")} sets -{flag} on {Name}, {user}");
            }

            public void Broadcast(string line, string except = null)
            {
                foreach (var user in Users)
                {
                    if (user.Nickname != except && user.Nickname != null && !line.StartsWith($":{user.Prefix()} ") && user.Authorized)
                    {
                        user.Send(line.Replace(Server.NICKNAME, user.Nickname));
                    }
                }
            }

            public void BroadcastMessage(User from, string mesg, string except = null)
            {
                foreach (var user in Users)
                {
                    if (user.Nickname != except && user.Nickname != null && from.Prefix() != user.Prefix() && user.Authorized)
                    {
                        if (user.IsRevolution)
                        {
                            user.Send($":{from.Prefix()} PRIVMSG {Name} {from.Nickname} :{from.Username} :{mesg}");
                        }
                        else
                        {
                            user.Send($":{from.Prefix()} PRIVMSG {Name} :{mesg}");
                        }
                    }
                }
            }

            public void SendMessage(User from, string mesg, string except = null)
            {
                foreach (var user in Users)
                {
                    if (user.Nickname != except && user.Nickname != null && user.Authorized)
                    {
                        if (user.IsRevolution)
                        {
                            user.Send($":{from.Prefix()} PRIVMSG {Name} {from.Nickname} :{from.Username} :{mesg}");
                        }
                        else
                        {
                            user.Send($":{from.Prefix()} PRIVMSG {Name} :{mesg}");
                        }
                    }
                }
            }

            public void BroadcastNotice(User from, string mesg, string except = null)
            {
                foreach (var user in Users)
                {
                    if (user.Nickname != except && user.Nickname != null && from.Prefix() != user.Prefix() && user.Authorized)
                    {
                        if (user.IsRevolution)
                        {
                            user.Send($":{from.Prefix()} NOTICE {Name} {from.Nickname} :{from.Username} :{mesg}");
                        }
                        else
                        {
                            user.Send($":{from.Prefix()} NOTICE {Name} :{mesg}");
                        }
                    }
                }
            }

            public void Send(string line)
            {
                foreach (var user in Users)
                {
                    if (user.Authorized)
                        user.Send(line.Replace(Server.NICKNAME, user.Nickname));
                }
            }
        }

        static string base64urlencode(byte[] arg)
        {
            string s = Convert.ToBase64String(arg); // Regular base64 encoder
            s = s.Split('=')[0]; // Remove any trailing '='s
            s = s.Replace('+', '-'); // 62nd char of encoding
            s = s.Replace('/', '_'); // 63rd char of encoding
            return s;
        }

        public class User
        {
            public string Nickname;
            public string Username;
            public string Realname;
            public string Hostname;
            public string Servername;
            public bool Authorized;
            public string Pass;
            public event Action<User, string> MessageReceived;
            public event Action<User> Quit;

            public string Pinged;
            public bool Ponged;

            public bool IsRevolution;

            private TcpClient connection;
            public StreamReader StreamReader;
            public StreamWriter StreamWriter;
            public User(TcpClient forClient)
            {
                this.connection = forClient;
                StreamReader = new StreamReader(connection.GetStream());
                StreamWriter = new StreamWriter(connection.GetStream());

                IsRevolution = false;
                Authorized = false;

                //connection.GetStream().BeginRead(new byte[] { 0 }, 0, 0, read, null);
                var thCancel = false;
                var th = new Thread(() =>
                {
                    try
                    {
                        var line = "";
                        while (true)
                        {
                            if (thCancel) throw new OperationCanceledException();
                            line = StreamReader.ReadLine();
                            if (thCancel) throw new OperationCanceledException();
                            // Console.WriteLine(line);
                            if (line == null)
                            {
                                Quit?.Invoke(this);
                                break;
                            }
                            else
                            {
                                MessageReceived?.Invoke(this, line);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                        if (ex.GetType().Name != "OperationCanceledException")
                        {
                            Close();
                        }
                    }
                });
                th.Start();
                Quit += (User s) =>
                {
                    thCancel = true;
                    try
                    {
                        th.Abort();
                    }
                    catch (Exception)
                    {

                    }
                };
            }

            private readonly string randomShit = base64urlencode(System.Text.Encoding.ASCII.GetBytes(Guid.NewGuid().ToString().Replace("-", "").Substring(0, 10)));
            public string Prefix(bool acceptHideIPSetting = true)
            {
                return (Nickname is string && Username is string ? $"{Nickname}!{Username}@" : "") + (Configuration.HideIPs && acceptHideIPSetting ? randomShit : connection.Client.RemoteEndPoint.ToString());
            }

            public void Notice(string channelName, string message, User sender = null)
            {
                if (sender is User)
                {
                    Send($":{sender.Prefix()} NOTICE {channelName}{(IsRevolution ? $" {sender.Nickname} :{sender.Username}" : "")} :{message}");
                }
                else
                {
                    Send($":Serv NOTICE {channelName}{(IsRevolution ? $" Serv :Serv" : "")} :{message}");
                }
            }

            private void read(IAsyncResult ar)
            {
                try
                {
                    var reader = new StreamReader(connection.GetStream());

                    connection.GetStream().BeginRead(new byte[] { 0 }, 0, 0, read, null);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    Quit?.Invoke(this);
                }
            }

            List<string> delayedWrite = new List<string>();

            public void Send(string line)
            {
                // Console.WriteLine(line);
                delayedWrite.Add(line);
            }

            public void Flush()
            {
                try
                {
                    if (delayedWrite.Count == 0) return;
                    StreamWriter.Write(string.Join("\r\n", delayedWrite) + "\r\n");
                    delayedWrite.Clear();
                    StreamWriter.Flush();
                }
                catch (Exception)
                {
                    Quit?.Invoke(this);
                }
            }

            public void Close()
            {
                try
                {
                    Quit?.Invoke(this);
                }
                catch (Exception)
                {

                }
                try
                {
                    connection.Close();
                    connection = null;
                }
                catch (Exception)
                {

                }
            }
        }
    }
}
