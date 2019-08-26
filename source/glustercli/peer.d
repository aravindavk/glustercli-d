module glustercli.peer;

import glustercli.peer_parser;
import glustercli.utils;

void peerAdd(string address)
{
    auto cmd = ["peer", "probe", address];
    executeGlusterCmd(cmd);
}

void peerRemove(string address)
{
    auto cmd = ["peer", "detach", address];
    executeGlusterCmd(cmd);
}

Peer[] peerStatus()
{
    auto cmd = ["pool", "list"];
    auto outlines = executeGlusterCmdXml(cmd);

    return parsePeersFromPoolList(outlines);
}
