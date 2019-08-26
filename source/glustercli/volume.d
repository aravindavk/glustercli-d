module glustercli.volume;

import glustercli.volume_parser;
import glustercli.utils;

struct VolumeCreateOptions
{
    int replicaCount;
    int arbiterCount;
    int disperseCount;
    int disperseRedundancyCount;
    int disperseDataCount;
    string transport;
    bool force;
}

void volumeCreate(string volname, string[] bricks, VolumeCreateOptions opts)
{
    import std.format;

    auto cmd = ["volume", "create", volname];
    if (opts.replicaCount > 0)
        cmd ~= ["replica", format!"%d"(opts.replicaCount)];

    if (opts.arbiterCount > 0)
        cmd ~= ["arbiter", format!"%d"(opts.arbiterCount)];

    if (opts.disperseCount > 0)
        cmd ~= ["disperse", format!"%d"(opts.disperseCount)];
    
    if (opts.disperseDataCount > 0)
        cmd ~= ["disperse-data", format!"%d"(opts.disperseDataCount)];

    if (opts.disperseRedundancyCount > 0)
        cmd ~= ["redundancy", format!"%d"(opts.disperseRedundancyCount)];

    if (opts.transport != "tcp" && opts.transport != "")
        cmd ~= ["transport", opts.transport];

    cmd ~= bricks;

    if (opts.force)
        cmd ~= ["force"];

    executeGlusterCmd(cmd);
}

void volumeStart(string volname, bool force=false)
{
    auto cmd = ["volume", "start", volname];
    if (force)
        cmd ~= ["force"];

    executeGlusterCmd(cmd);
}

void volumeStop(string volname, bool force=false)
{
    auto cmd = ["volume", "stop", volname];
    if (force)
        cmd ~= ["force"];

    executeGlusterCmd(cmd);
}

void volumeRestart(string volname)
{
    stopVolume(volname, true);
    startVolume(volname, true);
}

void volumeDelete(string volname)
{
    executeGlusterCmd(["volume", "delete", volname]);
}

// void getVolumeOption(string volname, string optname)
// {
//     TODO: parse volume option
//     auto cmd = ["volume", "get", volname, optname];
// }

void volumeSetOption(string volname, string[string] opts)
{
    auto cmd = ["volume", "set", volname];
    foreach(keyval; opts.byKeyValue())
        cmd ~= [keyval.key, keyval.value];

    executeGlusterCmd(cmd);
}

void volumeResetOption(string volname, string opt, bool force)
{
    auto cmd = ["reset", volname];

    if (opt != "")
        cmd ~= opt;

    if (force)
        cmd ~= "force";

    executeGlusterCmd(cmd);
}

void volumeResetOption(string volname, string opt)
{
    volumeResetOption(volname, opt, false);
}

void volumeResetOption(string volname, bool force)
{
    volumeResetOption(volname, "", force);
}

void volumeResetOption(string volname)
{
    volumeResetOption(volname, "", false);
}

string[] volumeList()
{
    return executeGlusterCmd(["volume", "list"]);
}

Volume[] volumeInfo(string volname)
{
    auto cmd = ["volume", "info"];
    if (volname != "")
        cmd ~= volname;

    auto outlines = executeGlusterCmdXml(cmd);
    Volume[] volumes;
    parseVolumesFromVolinfo(outlines, volumes);
    return volumes;
}

Volume[] volumeStatus(string volname)
{
    auto cmd = [
        "volume", "status",
        volname == "" ? "all" : volname,
        "detail"
        ];

    auto outlines = executeGlusterCmdXml(cmd);

    auto volumes = volumeInfo(volname);
    parseVolumesFromStatus(outlines, volumes);
    return volumes;
}

Volume[] volumeInfo()
{
    return volumeInfo("");
}

Volume[] volumeStatus()
{
    return volumeStatus("");
}
