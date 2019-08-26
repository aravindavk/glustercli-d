module glustercli;

public import glustercli.volume;
public import glustercli.volume_parser: Volume;
public import glustercli.peer;
public import glustercli.peer_parser: Peer;

void voltypeTest(ref Volume volinfo, string expectedType)
{
    assert(volinfo.type == expectedType);
}

unittest
{
    import std.process: environment;
    import std.format: format;

    string hostname = environment.get("GLUSTER_HOST");
    string brickRootdir = environment.get("BRICK_ROOT");
    string volname = "gv1";
    auto brick = format!"%s:%s/%s/brick%d/brick"(hostname, brickRootdir, volname, 1);
    
    VolumeCreateOptions opts = {force: true};
    volumeCreate(volname, [brick], opts);
    auto vollist = volumeList();
    assert(vollist.length == 1);
    assert(vollist[0] == volname);
    auto volinfo = volumeInfo(volname);
    voltypeTest(volinfo[0], "Distribute");
    
    // 1 brick distribute - gv1
    // 1 brick distribute without force
    // 3 brick distribute - gv2
    // 3 brick replica - gv3
    // 3 brick disperse - gv4
    // 3 brick arbiter - gv5
    // 3x2 brick dist-rep - gv6
    // 3x2 brick dist-disp - gv7
    // 3x2 brick dist-arbiter - gv8

    // For each Volume test the following
    // Present in Vollist
    // From Volinfo,
    //     check volume name
    //     check volume type
    //     check number of bricks
    //     brick position validate
    //     subvol type
    //     number of subvols
    //     status == created
    //     validate replicaCount and other counts

    // For each Volume
    // Volume start
    //     check status == started
    //     brick ports check
    //     brick status check
    // Kill a brick and check brick status
    // Volume start force and check brick status

    // Volume stop
    //    check status == stopped

    // Volume delete
    //     Check Not in volume list


    // Cleanup
    
}
