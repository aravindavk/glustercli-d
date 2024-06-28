module glustercli.peers;

import std.array;

import yxml;

struct Peer
{
    string address;
    string id;
    string state;
}

/*

Example output:
---
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cliOutput>
  <opRet>0</opRet>
  <opErrno>0</opErrno>
  <opErrstr/>
  <peerStatus>
    <peer>
      <uuid>1b58cfc0-15ed-40b8-be28-f7c341250777</uuid>
      <hostname>localhost</hostname>
      <connected>1</connected>
    </peer>
  </peerStatus>
</cliOutput>
---
 */
Peer[] parsePeersFromPoolList(string data, string localhostAddress)
{
    Peer[] peers;
    XmlDocument doc;
    doc.parse(data);

    XmlElement root = doc.root;

    XmlElement peerslist = root.firstChildByTagName("peerStatus");

    foreach (XmlElement e; peerslist.getChildrenByTagName("peer"))
    {
        Peer peer;

        peer.address = e.firstChildByTagName("hostname").textContent.dup;

        if (peer.address == "localhost")
            peer.address = localhostAddress;

        peer.id = e.firstChildByTagName("uuid").textContent.dup;
        XmlElement connected = e.firstChildByTagName("connected");
        peer.state = connected.textContent == "1" ? "Connected" : "Disconnected";

        peers ~= peer;
    }

    return peers;
}

mixin template peersFunctions()
{
    void addPeer(string address)
    {
        auto cmd = ["peer", "probe", address];
        executeGlusterCmd(cmd);
    }

    Peer[] listPeers()
    {
        auto cmd = ["pool", "list"];
        auto outlines = executeGlusterCmdXml(cmd);

        return parsePeersFromPoolList(outlines, settings.localhostAddress);
    }

    void deletePeer(string address)
    {
        auto cmd = ["peer", "detach", address];
        executeGlusterCmd(cmd);
    }
}
