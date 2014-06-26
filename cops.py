#!/usr/bin/env python
# encoding: utf-8

import logging
from plumbum import cli
from plumbum.machines import SshMachine
from plumbum import local
from plumbum import FG
from plumbum import BG

# Initialize
logger = logging.getLogger("ClusterOps")
logger.setLevel(logging.INFO)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

class ClusterOps(cli.Application):
    """ Azure Cluster Operations Tool"""
    VERSION = "0.1"

    @cli.switch(['-d', '--debug'], help='Enable debug logging')
    def set_debug(self):
        """Sets debug mode"""
        # logging.basicConfig(level = logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel( logging.DEBUG )

    @cli.switch(['-s', '--simple'], help='sets the simple log format')
    def set_simple(self):
        for handler in logger.handlers:
            formatter = logging.Formatter('%(message)s')
            handler.setFormatter(formatter)

    _user = cli.SwitchAttr( ['-u', '--user-name'], str, mandatory = True,
            help='Username to use to login to remote hosts')

    _keyfile = cli.SwitchAttr( ['-k', '--key-file'], cli.ExistingFile,
             help='`pem` file for login to remote hosts')

    _host_list = cli.SwitchAttr( ['-n', '--host-name'], str, group='Host List',
            excludes=[ '-f', '--host-file' ], list=True, help='hosts to operate upon')

    _host_file = cli.SwitchAttr( ['-f', '--host-file'], cli.ExistingFile, group='Host List',
            excludes=[ '-n', '--host-name' ], help='hosts to operate upon')

    def load_hosts(self):
        if self._host_list and self._host_file:
            print "Only one of --host-name and --host-file can be created"
            return 1
        elif self._host_list:
            self._hosts = [i.strip() for i in self._host_list]
        elif self._host_file:
            with open( self._host_file._path, 'r' ) as of:
                self._hosts = [line.strip() for line in of]
        else:
            print "Specify --host-name OR --host-file"
            return 1

        logger.debug( "Hosts loaded - %s " % self._hosts )

    def main(self, *args):
        rc = self.load_hosts()
        if rc:
            return 1
        if args:
            print "Unknown command %r" % (args[0],)
            return 1   # error exit code
        if not self.nested_command:           # will be ``None`` if no sub-command follows
            print "No command given"
            return 1   # error exit code

@ClusterOps.subcommand("copy")
class ClusterOpsCopy(cli.Application):
    """Copy the specified file to each node in the hosts list"""

    def main(self, local_file, remote_path):
        for host in self.parent._hosts:
            hn = host
            port = 22
            toks = host.split(":")
            if len(toks) == 2:
                hn = toks[0]
                port = int(toks[1])

            logger.debug('Connecting to host %s' % host)
            with SshMachine( hn, port=port, user=self.parent._user,
                    keyfile=self.parent._keyfile,
                    ssh_opts=('-t', '-o StrictHostKeychecking=no' )
                    ) as rem:
                logger.info('Copying local file: %s to remote path:%s:%s' % (local_file, host, remote_path) )
                rem.upload( local_file, remote_path )

@ClusterOps.subcommand("sudo")
class ClusterOpsRun(cli.Application):
    """ Executes the command with the specified arguments as sudo"""
    def main(self, command_path, *args):
        for host in self.parent._hosts:
            hn = host
            port = 22
            toks = host.split(":")
            if len(toks) == 2:
                hn = toks[0]
                port = int(toks[1])

            logger.debug('Connecting to host %s' % host)
            with SshMachine( hn, port=port, user=self.parent._user,
                    keyfile=self.parent._keyfile,
                    ssh_opts=('-tt', '-o StrictHostKeychecking=no')
                    ) as rem:

                echo = local[ 'echo' ]
                sudo = rem[ 'sudo' ]
                comm = sudo[ command_path, args ]

                logger.debug('Executing [ host=%s command="%s" ]' % (host, comm) )
                logger.info('Executing [ host=%s command="%s" ]' % (host, command_path + " ".join( args )) )
                print comm()

if __name__ == "__main__":
    ClusterOps.run()
