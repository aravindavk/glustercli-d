module glustercli.volumes;

import std.array;
import std.conv;
import std.math.rounding;

import yxml;

import glustercli.peers;

const HEALTH_UP = "Up";
const HEALTH_DOWN = "Down";
const HEALTH_PARTIAL = "Partial";
const HEALTH_DEGRADED = "Degraded";

const STATE_CREATED = "Created";
const STATE_STARTED = "Started";
const STATE_STOPPED = "Stopped";
const STATE_UNKNOWN = "Unknown";

const TYPE_REPLICATE = "Replicate";
const TYPE_DISPERSE  = "Disperse";

struct Brick
{
    Peer peer;
    string path;
    string state = STATE_UNKNOWN;
    string health = HEALTH_DOWN;
    bool arbiter = false;
    int port;
    string pid;
    int blockSize;
    string device;
    string fsName;
    string mntOptions;
    ulong sizeUsed;
    ulong sizeTotal;
    ulong sizeFree;
    ulong inodesUsed;
    ulong inodesTotal;
    ulong inodesFree;
}

struct DistributeGroup
{
    string type;
    string health;
    Brick[] bricks;
    int upBricks;
    int replicaCount;
    int arbiterCount;
    int disperseCount;
    int disperseRedundancyCount;
    ulong sizeUsed;
    ulong sizeTotal;
    ulong sizeFree;
    ulong inodesUsed;
    ulong inodesTotal;
    ulong inodesFree;
}

struct Volume
{
    string id;
    string name;
    string type;
    string state;
    string health;
    int snapshots;
    int upDistributeGroups;
    int replicaCount;
    int arbiterCount;
    int disperseCount;
    int disperseRedundancyCount;
    DistributeGroup[] distributeGroups;
    ulong sizeUsed;
    ulong sizeTotal;
    ulong sizeFree;
    ulong inodesUsed;
    ulong inodesTotal;
    ulong inodesFree;
}

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

DistributeGroup[] fromBricks(Brick[] bricks, int distCount, int replicaCount)
{
    DistributeGroup[] distributeGroups;
    auto distGrpBricksCount = bricks.length / distCount;
    foreach (idx; 0 .. distCount)
    {
        DistributeGroup distGroup;

        foreach (bidx; 0 .. distGrpBricksCount)
        {
            distGroup.bricks ~= bricks[idx + bidx];
        }

        distributeGroups ~= distGroup;
    }

    return distributeGroups;
}

Volume[] parseVolumeInfo(string data)
{
    Volume[] volumes;
    XmlDocument doc;
    doc.parse(data);

    XmlElement root = doc.root;

    XmlElement volInfo = root.firstChildByTagName("volInfo");
    XmlElement vols = volInfo.firstChildByTagName("volumes");

    foreach (XmlElement e; vols.getChildrenByTagName("volume"))
    {
        Volume volume;
        Brick[] bricks;

        volume.name = e.firstChildByTagName("name").textContent.dup;
        volume.id = e.firstChildByTagName("id").textContent.dup;
        volume.state = e.firstChildByTagName("statusStr").textContent.dup;
        volume.type = e.firstChildByTagName("typeStr").textContent.dup;
        volume.snapshots = e.firstChildByTagName("snapshotCount").textContent.to!int;
        auto replicaCount = e.firstChildByTagName("replicaCount").textContent.to!int;
        auto distCount = e.firstChildByTagName("distCount").textContent.to!int;

        auto bricksList = e.firstChildByTagName("bricks");

        foreach (XmlElement brickEle; bricksList.getChildrenByTagName("brick"))
        {
            Brick brick;

            auto parts = brickEle.firstChildByTagName("name").textContent.dup.split(":");
            brick.peer.address = parts[0].to!string;
            brick.path = parts[1].to!string;
            brick.peer.id = brickEle.firstChildByTagName("hostUuid").textContent.dup;
            auto arbiter = brickEle.firstChildByTagName("isArbiter").textContent.dup;
            if (arbiter == "1")
                brick.arbiter = true;

            bricks ~= brick;
        }

        volume.distributeGroups = fromBricks(bricks, distCount, replicaCount);

        volumes ~= volume;
    }

    return volumes;
}

Volume updateHealth(Volume volume)
{
    if (volume.state != STATE_STARTED)
        return volume;

    volume.health = HEALTH_UP;
    volume.upDistributeGroups = 0;
    foreach(group; volume.distributeGroups)
    {
        // One subvol down means the Volume is degraded
        if (group.health == HEALTH_DOWN)
            volume.health = HEALTH_DEGRADED;
        
        // If Volume is not yet degraded, then it
        // may be same as subvolume health
        if (group.health == HEALTH_PARTIAL && volume.health != HEALTH_DEGRADED)
            volume.health = group.health;

        if (group.health != HEALTH_DOWN)
            volume.upDistributeGroups += 1;
    }

    if (volume.upDistributeGroups == 0)
        volume.health = HEALTH_DOWN;

    return volume;
}

Brick updateStatus(Brick brick, Brick[string] bricks)
{
    Brick* b = brick.peer.address ~ ":" ~ brick.path in bricks;

    if (b is null)
        return brick;

    return *b;
}

DistributeGroup updateHealth(DistributeGroup group)
{
    foreach(brick; group.bricks)
    {
        if (brick.state == HEALTH_UP)
            group.upBricks += 1;
    }

    group.health = HEALTH_UP;
    if (group.upBricks != group.bricks.length)
    {
        group.health = HEALTH_DOWN;
        if (group.type == TYPE_REPLICATE && group.upBricks >= (group.replicaCount/2.0).ceil)
            group.health = HEALTH_PARTIAL;

        // If down bricks are less than or equal to redudancy count
        if (group.type == TYPE_DISPERSE && (group.bricks.length - group.upBricks) <= group.disperseRedundancyCount)
            group.health = HEALTH_PARTIAL;
    }

    return group;
}

DistributeGroup updateUtilization(DistributeGroup group)
{
    foreach(brick; group.bricks)
    {
        if (brick.arbiter)
            continue;
        
        if (brick.sizeUsed >= group.sizeUsed)
            group.sizeUsed = brick.sizeUsed;

        if (brick.inodesUsed >= group.inodesUsed)
            group.inodesUsed = brick.inodesUsed;

        if (group.sizeTotal == 0 || (brick.sizeTotal < group.sizeTotal && brick.sizeTotal > 0))
            group.sizeTotal = brick.sizeTotal;

        if (group.inodesTotal == 0 || (brick.inodesTotal < group.inodesTotal && brick.inodesTotal > 0))
            group.inodesTotal = brick.inodesTotal;
    }

    if (group.type == TYPE_DISPERSE)
    {
        group.sizeUsed = group.sizeUsed * (group.disperseCount - group.disperseRedundancyCount);
        group.sizeTotal = group.sizeTotal * (group.disperseCount - group.disperseRedundancyCount);
        group.inodesUsed = group.inodesUsed * (group.disperseCount - group.disperseRedundancyCount);
        group.inodesTotal = group.inodesTotal * (group.disperseCount - group.disperseRedundancyCount);
    }

    group.sizeFree = group.sizeTotal - group.sizeUsed;
    group.inodesFree = group.inodesTotal - group.inodesUsed;

    return group;
}
    
Volume updateUtilization(Volume volume)
{
    foreach(group; volume.distributeGroups)
    {
        volume.sizeTotal += group.sizeTotal;
        volume.sizeUsed += group.sizeUsed;
        volume.sizeFree += volume.sizeTotal - volume.sizeUsed;
        volume.inodesTotal += group.inodesTotal;
        volume.inodesUsed += group.inodesUsed;
        volume.inodesFree += volume.inodesTotal - volume.inodesUsed;
    }
    return volume;
}

Brick[string] parseVolumeStatus(string data)
{
    Brick[string] bricks;
    XmlDocument doc;
    doc.parse(data);

    XmlElement root = doc.root;

    XmlElement volStatus = root.firstChildByTagName("volStatus");
    XmlElement vols = volStatus.firstChildByTagName("volumes");
    foreach (XmlElement e; vols.getChildrenByTagName("volume"))
    {
        foreach (XmlElement brickEle; e.getChildrenByTagName("node"))
        {
            Brick brick;

            brick.peer.address = brickEle.firstChildByTagName("hostname").textContent.dup;
            brick.path = brickEle.firstChildByTagName("path").textContent.dup;
            brick.peer.id = brickEle.firstChildByTagName("peerid").textContent.dup;
            auto status = brickEle.firstChildByTagName("status").textContent.dup;
            brick.state = status == "1" ? "Up" : "Down";
            brick.pid = brickEle.firstChildByTagName("pid").textContent.dup;
            brick.sizeTotal = brickEle.firstChildByTagName("sizeTotal").textContent.dup.to!ulong;
            brick.sizeFree = brickEle.firstChildByTagName("sizeFree").textContent.dup.to!ulong;
            brick.inodesTotal = brickEle.firstChildByTagName("inodesTotal").textContent.dup.to!ulong;
            brick.inodesFree = brickEle.firstChildByTagName("inodesFree").textContent.dup.to!ulong;
            brick.device = brickEle.firstChildByTagName("device").textContent.dup;
            brick.blockSize = brickEle.firstChildByTagName("blockSize").textContent.dup.to!int;
            brick.fsName = brickEle.firstChildByTagName("fsName").textContent.dup;
            brick.mntOptions = brickEle.firstChildByTagName("mntOptions").textContent.dup;
            brick.sizeUsed = brick.sizeTotal - brick.sizeFree;
            brick.inodesUsed = brick.inodesTotal - brick.inodesFree;

            bricks[brick.peer.address ~ ":" ~ brick.path] = brick;
        }
    }
    return bricks;
}

unittest
{
    import std.file;

    string infoXml = readText("tests/samples/volume_info.xml");
    auto volumes = parseVolumeInfo(infoXml);

    assert(volumes.length == 1, "");
    assert(volumes[0].name == "vol1", "Volume name is not \"vol1\"");
}

mixin template volumesFunctions()
{
    void createVolume(string name, string[] bricks, VolumeCreateOptions opts)
    {
        import std.format;

        auto cmd = ["volume", "create", name];
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

    private void startStopVolume(string name, bool start = true, bool force = false)
    {
        string action = start ? "start" : "stop";
        auto cmd = ["volume", action, name];
        if (force)
            cmd ~= ["force"];

        executeGlusterCmd(cmd);
    }

    void startVolume(string name, bool force = false)
    {
        startStopVolume(name, true, force);
    }

    void stopVolume(string name, bool force = false)
    {
        startStopVolume(name, false, force);
    }

    Volume[] listVolumes(bool status = false)
    {
        auto cmd = ["volume", "info"];

        auto outlines = executeGlusterCmdXml(cmd);
        auto volumeInfos = parseVolumeInfo(outlines);

        if (!status)
            return volumeInfos;

        auto statusCmd = ["volume", "status", "all", "detail"];
        auto statusOutlines = executeGlusterCmdXml(statusCmd);
        auto brickStatus = parseVolumeStatus(statusOutlines);

        Volume[] volumes;
        foreach(volume; volumeInfos)
        {
            DistributeGroup[] groups;
            foreach(group; volume.distributeGroups)
            {
                Brick[] bricks;
                foreach(brick; group.bricks)
                    bricks ~= brick.updateStatus(brickStatus);

                group.bricks = bricks;
                group = group
                    .updateUtilization
                    .updateHealth;

                groups ~= group;
            }
            volume.distributeGroups = groups;
            volume = volume
                .updateUtilization
                .updateHealth;

            volumes ~= volume;
        }

        return volumes;
    }

    Volume getVolume(string name, bool status = false)
    {
        auto cmd = ["volume", "info", name];

        auto outlines = executeGlusterCmdXml(cmd);
        auto vols = parseVolumeInfo(outlines);
        return vols[0];
        if (!status)
            return vols[0];

        auto statusCmd = ["volume", "status", name, "detail"];
        auto statusOutlines = executeGlusterCmdXml(statusCmd);
        auto brickStatus = parseVolumeStatus(statusOutlines);

        Volume volume = vols[0];
        DistributeGroup[] groups;
        foreach(group; volume.distributeGroups)
        {
            Brick[] bricks;
            foreach(brick; group.bricks)
                bricks ~= brick.updateStatus(brickStatus);

            group.bricks = bricks;
            group = group
                .updateUtilization
                .updateHealth;
            
            groups ~= group;
        }
        volume.distributeGroups = groups;
        volume = volume
            .updateUtilization
            .updateHealth;

        return volume;
    }

    void deleteVolume(string name)
    {
        executeGlusterCmd(["volume", "delete", name]);
    }
}
