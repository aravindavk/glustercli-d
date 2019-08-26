module glustercli.peer_parser;

import std.process;
import std.json;
import std.string;

import arsd.dom;

enum StateOnline = "Online";
enum StateOffline = "Offline";

struct Peer
{
    string id;
    string address;
    string state;

    JSONValue toJson()
    {
        return JSONValue(
            [
                "id": JSONValue(id),
                "address": JSONValue(address),
                "state": JSONValue(state)
                ]
            );
    }
}


Peer[] parsePeersFromPoolList(string[] output)
{
    Peer[] peers;
    auto document = new Document(output.join(""));
    auto elements = document.getElementsByTagName("peer");
    foreach(ele; elements)
    {
        auto peer = Peer();
        peer.state = StateOffline;
        peer.id = ele.querySelector("uuid").innerText;
        peer.address = ele.querySelector("hostname").innerText;
        if (ele.querySelector("connected").innerText == "1")
            peer.state = StateOnline;

        if (peer.address == "localhost")
            peer.address = environment.get("GLUSTER_HOST", "localhost");
       
        peers ~= peer;
    }
    return peers;
}
