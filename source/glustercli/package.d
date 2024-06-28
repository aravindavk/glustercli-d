module glustercli;

public
{
    import glustercli.helpers;
    import glustercli.peers;
    import glustercli.volumes;
}

struct GlusterCLISettings
{
    string glusterCommand = "/usr/sbin/gluster";
    string localhostAddress;
}

class GlusterCLI
{
    GlusterCLISettings settings;

    this(GlusterCLISettings opts)
    {
        this.settings = opts;
    }

    mixin commandHelpers;
    mixin peersFunctions;
    mixin volumesFunctions;
}
