module glustercli.utils;

import std.string;
import std.process;

class GlusterCommandException : Exception
{
    this(string msg, string file = __FILE__, size_t line = __LINE__) {
        super(msg, file, line);
    }
}

string glusterCommand = "gluster";

void setGlusterCommand(string cmd)
{
    glusterCommand = cmd;
}

string[] executeGlusterCmd(string[] inputCmd, bool xml)
{
    auto cmd = [glusterCommand, "--mode=script"];
    if (xml)
        cmd ~= ["--xml"];

    cmd ~= inputCmd;

    string[] outlines;
    auto pipes = pipeProcess(cmd, Redirect.stdout | Redirect.stderr);
    auto returnCode = wait(pipes.pid);
    foreach (line; pipes.stdout.byLine) outlines ~= line.idup;
    if (returnCode != 0)
    {
        string[] errlines;
        foreach (line; pipes.stderr.byLine) errlines ~= line.idup;
        throw new GlusterCommandException(cmd.join(" ") ~ ": " ~ errlines.join("\n"));
    }

    return outlines;
}

string[] executeGlusterCmd(string[] inputCmd)
{
    return executeGlusterCmd(inputCmd, false);
}

string[] executeGlusterCmdXml(string[] inputCmd)
{
    return executeGlusterCmd(inputCmd, true);
}
