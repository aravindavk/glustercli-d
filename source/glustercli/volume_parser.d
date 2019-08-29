module glustercli.volume_parser;

import std.process;
import std.string;
import std.conv;
import std.json;
import std.algorithm: map;
import std.array: array;

import arsd.dom;

enum VolumeTypeDist = "Distribute";
enum VolumeTypeRep = "Replicate";
enum VolumeTypeDisp = "Disperse";
enum VolumeTypeDistRep = "Distributed Replicate";
enum VolumeTypeDistDisp = "Distributed Disperse";

enum SubvolTypeDist = "Distribute";
enum SubvolTypeRep = "Replicate";
enum SubvolTypeDisp = "Disperse";

enum BrickTypeDefault = "Brick";
enum BrickTypeArbiter = "Arbiter";

enum HealthUp = "up";
enum HealthDown = "down";
enum HealthPartial = "partial";
enum HealthDegraded = "degraded";

enum StateCreated = "Created";
enum StateStarted = "Started";
enum StateStopped = "Stopped";

struct Volume
{
    string name;
    string type;
    string state;
    string health;
    string id;
    int numSubvols;
    int numBricks;
    int replicaCount;
    int arbiterCount;
    int disperseCount;
    int disperseRedundancyCount;
    string transport;
    SubVolume[] subvols;
    ulong sizeTotal;
    ulong sizeUsed;
    ulong inodesTotal;
    ulong inodesUsed;
    Option[] options;
    JSONValue toJson()
    {
        return JSONValue(
            [
                "name": JSONValue(name),
                "type": JSONValue(type),
                "state": JSONValue(state),
                "health": JSONValue(health),
                "id": JSONValue(id),
                "num_subvols": JSONValue(numSubvols),
                "num_bricks": JSONValue(numBricks),
                "replica_count": JSONValue(replicaCount),
                "arbiter_count": JSONValue(arbiterCount),
                "disperse_count": JSONValue(disperseCount),
                "disperse_redundancy_count": JSONValue(disperseRedundancyCount),
                "transport": JSONValue(transport),
                "subvols": JSONValue(subvols.map!(subvol => subvol.toJson).array),
                "size_total": JSONValue(sizeTotal),
                "size_used": JSONValue(sizeUsed),
                "inodes_total": JSONValue(inodesTotal),
                "inodes_used": JSONValue(inodesUsed),
                "options": JSONValue(options.map!(opt => opt.toJson).array),
                ]
            );
    }
}

struct Option
{
    string name;
    string value;
    string volumeId;
    JSONValue toJson()
    {
        return JSONValue(
            [
                "name": JSONValue(name),
                "value": JSONValue(value),
                "volumes_id": JSONValue(volumeId)
            ]
        );
    }
}

struct SubVolume
{
    string id;
    string health;
    int replicaCount;
    int arbiterCount;
    int disperseCount;
    int disperseRedundancyCount;
    string type;
    Brick[] bricks;
    string volumeId;
    ulong numBricks;
    JSONValue toJson()
    {
        return JSONValue(
            [
                "id": JSONValue(id),
                "health": JSONValue(health),
                "replica_count": JSONValue(replicaCount),
                "arbiter_count": JSONValue(arbiterCount),
                "disperse_count": JSONValue(disperseCount),
                "disperse_redundancy_count": JSONValue(disperseRedundancyCount),
                "type": JSONValue(type),
                "bricks": JSONValue(bricks.map!(brick => brick.toJson).array),
                "num_bricks": JSONValue(numBricks),
                "volumes_id": JSONValue(volumeId)
            ]
        );
    }
}

struct Brick
{
    string host;
    string path;
    string nodeid;
    string state;
    string type;
    int port;
    int pid;
    ulong sizeTotal;
    ulong sizeUsed;
    ulong inodesTotal;
    ulong inodesUsed;
    string fsType;
    string device;
    int blockSize;
    string mountOptions;
    string volumeId;
    string subvolId;
    JSONValue toJson()
    {
        return JSONValue(
            [
                "host": JSONValue(host),
                "path": JSONValue(path),
                "peers_id": JSONValue(nodeid),
                "state": JSONValue(state),
                "type": JSONValue(type),
                "port": JSONValue(port),
                "pid": JSONValue(pid),
                "size_total": JSONValue(sizeTotal),
                "size_used": JSONValue(sizeUsed),
                "inodes_total": JSONValue(inodesTotal),
                "inodes_used": JSONValue(inodesUsed),
                "fs": JSONValue(fsType),
                "device": JSONValue(device),
                "block_size": JSONValue(blockSize),
                "mount_options": JSONValue(mountOptions),
                "volumes_id": JSONValue(volumeId),
                "subvols_id": JSONValue(subvolId),
            ]
        );
    }
}

string transportType(string val)
{
    if (val == "0")
        return "TCP";
    else if (val == "1")
        return "RDMA";

    return "TCP,RDMA";
}

int parsePortOrPid(string val)
{
    try
    {
        return to!int(val);
    }
    catch (ConvException)
    {
        return -1;
    }
}

Brick[string] parseBricksFromVolStatus(Element[] elements)
{
    Brick[string] bricks;
    foreach(ele; elements)
    {
        auto brick = Brick();
        brick.host = ele.querySelector("hostname").innerText;
        brick.path = ele.querySelector("path").innerText;
        brick.nodeid = ele.querySelector("peerid").innerText;
        brick.fsType = ele.querySelector("fsName").innerText;
        brick.device = ele.querySelector("device").innerText;
        brick.blockSize = to!int(ele.querySelector("blockSize").innerText);
        brick.mountOptions = ele.querySelector("mntOptions").innerText;
        brick.state = (ele.querySelector("status").innerText == "1" ? HealthUp : HealthDown);
        brick.sizeTotal = to!ulong(ele.querySelector("sizeTotal").innerText);
        brick.sizeUsed = brick.sizeTotal - to!ulong(ele.querySelector("sizeFree").innerText);
        brick.inodesTotal = to!ulong(ele.querySelector("inodesTotal").innerText);
        brick.inodesUsed = brick.inodesTotal - to!ulong(ele.querySelector("inodesFree").innerText);
        brick.port = parsePortOrPid(ele.querySelector("port").innerText);
        brick.pid = parsePortOrPid(ele.querySelector("pid").innerText);

        bricks[brick.host ~ ":" ~ brick.path] = brick;
    }
    return bricks;
}


Option[] parseOptionsFromVolinfo(Volume vol, Element[] elements)
{
    Option[] options;
    foreach(ele; elements)
    {
        options ~= Option(
            ele.querySelector("name").innerText,
            ele.querySelector("value").innerText,
            vol.id
            );
    }
    return options;
}

Brick[] parseBricksFromVolinfo(Element[] elements)
{
    Brick[] bricks;
    foreach(ele; elements)
    {
        auto brick = Brick();
        auto hostAndPath = ele.querySelector("name").innerText.split(":");
        brick.path = hostAndPath[1];
        brick.host = hostAndPath[0];
        brick.nodeid = ele.querySelector("hostUuid").innerText;
        brick.type = BrickTypeDefault;
        brick.type = (ele.querySelector("isArbiter").innerText == "1" ? BrickTypeArbiter : BrickTypeDefault);

        brick.blockSize = 4096;
        brick.state = "Unknown";
        bricks ~= brick;
    }
    return bricks;
}

int getSubvolBricksCount(int replicaCount, int disperseCount)
{
	if (replicaCount > 0)
		return replicaCount;

    if (disperseCount > 0)
		return disperseCount;

	return 1;
}


string getSubvolType(string voltype)
{
	switch (voltype)
    {
	case VolumeTypeDistRep:
		return SubvolTypeRep;
	case VolumeTypeDistDisp:
		return SubvolTypeDisp;
	default:
		return voltype;
	}
}

void parseVolumesFromVolinfo(string[] output, ref Volume[] volumes)
{
    auto document = new Document(output.join(""));
    auto elements = document.getElementsByTagName("volume");
    foreach(ele; elements)
    {
        auto vol = Volume();
        vol.health = HealthDown;
        vol.name = ele.querySelector("name").innerText;
        vol.type = ele.querySelector("typeStr").innerText.replace("-", " ");
        vol.state = ele.querySelector("statusStr").innerText;
        vol.id = ele.querySelector("id").innerText;
        vol.numBricks = to!int(ele.querySelector("brickCount").innerText);
        vol.replicaCount = to!int(ele.querySelector("replicaCount").innerText);
        vol.disperseCount = to!int(ele.querySelector("disperseCount").innerText);
        vol.arbiterCount = to!int(ele.querySelector("arbiterCount").innerText);
        vol.disperseRedundancyCount = to!int(ele.querySelector("redundancyCount").innerText);
        vol.transport = transportType(ele.querySelector("transport").innerText);

        vol.options = parseOptionsFromVolinfo(vol, ele.getElementsByTagName("option"));
        
        // Parse the bricks and create subvol groups
        auto bricks = parseBricksFromVolinfo(ele.getElementsByTagName("brick"));
        auto subvolBricksCount = getSubvolBricksCount(vol.replicaCount, vol.disperseCount);
        vol.numSubvols = vol.numBricks / subvolBricksCount;
        
        foreach(sidx; 0 .. vol.numSubvols)
        {
            auto subvol = SubVolume();
            subvol.volumeId = vol.id;
            subvol.health = HealthDown;
            subvol.type = getSubvolType(vol.type);
            subvol.replicaCount = vol.replicaCount;
            subvol.arbiterCount = vol.arbiterCount;
            subvol.disperseCount = vol.disperseCount;
            subvol.disperseRedundancyCount = vol.disperseRedundancyCount;
            subvol.id = format!"%s-%s-%d"(vol.name, subvol.type.toLower, sidx);
            foreach(bidx; 0 .. subvolBricksCount)
            {
                subvol.bricks ~= bricks[sidx+bidx];
            }
            subvol.numBricks = bricks.length;
            vol.subvols ~= subvol;
        }
       
        volumes ~= vol;
    }
}

void mergeVolumeInfoAndStatus(ref Volume[] volumes, Brick[string] bricksdata)
{
    foreach(vol; volumes)
    {
        foreach(subvol; vol.subvols)
        {
            foreach(ref brick; subvol.bricks)
            {
                auto name = brick.host ~ ":" ~ brick.path;
                brick.volumeId = vol.id;
                brick.subvolId = subvol.id;
                if ((name in bricksdata) !is null)
                {
                    brick.fsType = bricksdata[name].fsType;
                    brick.device = bricksdata[name].device;
                    brick.blockSize = bricksdata[name].blockSize;
                    brick.mountOptions = bricksdata[name].mountOptions;
                    brick.state = bricksdata[name].state;
                    brick.sizeUsed = bricksdata[name].sizeUsed;
                    brick.sizeTotal = bricksdata[name].sizeTotal;
                    brick.inodesUsed = bricksdata[name].inodesUsed;
                    brick.inodesTotal = bricksdata[name].inodesTotal;
                    brick.port = bricksdata[name].port;
                    brick.pid = bricksdata[name].pid;
                }
            }
        }
    }
}

void updateVolumeUtilization(ref Volume[] volumes)
{
    foreach(ref vol; volumes)
    {
        foreach(subvol; vol.subvols)
        {
            ulong effectiveCapacityUsed, effectiveCapacityTotal;
            ulong effectiveInodesUsed, effectiveInodesTotal;

            foreach(brick; subvol.bricks)
            {
                if (brick.type != BrickTypeArbiter)
                {
                    if(brick.sizeUsed >= effectiveCapacityUsed)
                        effectiveCapacityUsed = brick.sizeUsed;
                
                    if (effectiveCapacityTotal == 0 || brick.sizeTotal <= effectiveCapacityTotal)
                        effectiveCapacityTotal = brick.sizeTotal;

                    if(brick.inodesUsed >= effectiveInodesUsed)
                        effectiveInodesUsed = brick.inodesUsed;
                
                    if (effectiveInodesTotal == 0 || brick.inodesTotal <= effectiveInodesTotal)
                        effectiveInodesTotal = brick.inodesTotal;
                }
            }
            if (subvol.type == SubvolTypeDisp)
            {
                // Subvol Size = Sum of size of Data bricks
                effectiveCapacityUsed = effectiveCapacityUsed * (subvol.disperseCount - subvol.disperseRedundancyCount);
                effectiveCapacityTotal = effectiveCapacityTotal * (subvol.disperseCount - subvol.disperseRedundancyCount);
                effectiveInodesUsed = effectiveInodesUsed * (subvol.disperseCount - subvol.disperseRedundancyCount);
                effectiveInodesTotal = effectiveInodesTotal * (subvol.disperseCount - subvol.disperseRedundancyCount);
            }

            vol.sizeTotal += effectiveCapacityTotal;
            vol.sizeUsed += effectiveCapacityUsed;
            vol.inodesTotal += effectiveInodesTotal;
            vol.inodesUsed += effectiveInodesUsed;
        }
    }
}

void updateVolumeHealth(ref Volume[] volumes)
{
    foreach(ref vol; volumes)
    {
        if (vol.state != StateStarted)
            continue;

        vol.health = HealthUp;
        auto upSubvols = 0;
        foreach(ref subvol; vol.subvols)
        {
            auto upBricks = 0;
            foreach(brick; subvol.bricks)
            {
                if (brick.state == HealthUp)
                    upBricks++;
            }
            subvol.health = HealthUp;
            if (subvol.bricks.length != upBricks)
            {
                subvol.health = HealthDown;
                if (subvol.type == SubvolTypeRep && upBricks >= (subvol.replicaCount/2 + 1))
                    subvol.health = HealthPartial;

                // If down bricks are less than or equal to redudancy count
                // then Volume is UP but some bricks are down
                if (subvol.type == SubvolTypeDisp &&
                    (subvol.bricks.length - upBricks) <= subvol.disperseRedundancyCount)
                    subvol.health = HealthPartial;

            }

            if (subvol.health == HealthDown)
                vol.health = HealthDegraded;

            if (subvol.health == HealthPartial && vol.health != HealthDegraded)
                vol.health = subvol.health;

            if (subvol.health != HealthDown)
                upSubvols++;
        }
        if (upSubvols == 0)
            vol.health = HealthDown;
    }
}


void parseVolumesFromStatus(string[] output, ref Volume[] volumes)
{
    auto document = new Document(output.join(""));
    auto bricksdata = parseBricksFromVolStatus(document.getElementsByTagName("node"));
    mergeVolumeInfoAndStatus(volumes, bricksdata);
    updateVolumeUtilization(volumes);
    updateVolumeHealth(volumes);
}
