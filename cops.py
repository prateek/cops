#!/usr/bin/env python
# encoding: utf-8

import logging
from plumbum import cli
from plumbum.machines import SshMachine
from plumbum import local
from plumbum import FG

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

    _user = cli.SwitchAttr( ['-u', '--user-name'], str, mandatory = True,
            help='Username to use to login to remote hosts')

    _keyfile = cli.SwitchAttr( ['-k', '--key-file'], cli.ExistingFile, mandatory = True,
            help='`pem` file for login to remote hosts')

    _host_list = cli.SwitchAttr( ['-n', '--host-name'], str, group='Host List',
            excludes=[ '-f', '--host-file' ], list=True, help='hosts to operate upon')

    _host_file = cli.SwitchAttr( ['-f', '--host-file'], cli.ExistingFile, group='Host List',
            excludes=[ '-n', '--host-name' ], help='hosts to operate upon')


    @cli.Flag(['-d', '--debug'], help='Enable debug logging')
    def set_debug(self):
        """Sets debug mode"""
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel( logging.DEBUG )

    def main(self, *args):
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
        scp = local[ 'scp' ]

        for host in hosts:
            logger.info('Copying local file: %s to remote path:%s:%s' % (local_file, host, remote_path) )
            copy = scp[ '-i', keyfile, local_file, '%s@%s:%s' % (self._user,host, remote_path) ]

            logger.debug(copy)
            copy()

@ClusterOps.subcommand("run")
class ClusterOpsPush(cli.Application):
    """ Executes the command with the specified arguments"""
    def main(self, command_path, *args):
        for host in hosts:
            logger.debug('Connecting to host %s' % host)
            with SshMachine( host, user=self._user, keyfile=self._keyfile, ssh_opts=('-t', )) as rem:
                comm = rem[ command_path ]
                arg_comm = comm[ args ]
                logger.info(' Executing [ host=%s command="%s" ] ' % (host, arg_comm) )
                arg_comm & FG

if __name__ == "__main__":
    ClusterOps.run()
