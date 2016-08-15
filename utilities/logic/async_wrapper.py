#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2012, Michael DeHaan <michael.dehaan@gmail.com>, and others
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
#

try:
    import json
except ImportError:
    import simplejson as json
import shlex
import os
import subprocess
import sys
import traceback
import signal
import time
import syslog

syslog.openlog('ansible-%s' % os.path.basename(__file__))
syslog.syslog(syslog.LOG_NOTICE, 'Invoked with %s' % " ".join(sys.argv[1:]))

def notice(msg):
    syslog.syslog(syslog.LOG_NOTICE, msg)

def daemonize_self():
    # daemonizing code: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/66012
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)
    except OSError:
        e = sys.exc_info()[1]
        sys.exit("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))

    # decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(int('022', 8))

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # print "Daemon PID %d" % pid
            sys.exit(0)
    except OSError:
        e = sys.exc_info()[1]
        sys.exit("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))

    dev_null = file('/dev/null','rw')
    os.dup2(dev_null.fileno(), sys.stdin.fileno())
    os.dup2(dev_null.fileno(), sys.stdout.fileno())
    os.dup2(dev_null.fileno(), sys.stderr.fileno())


def _run_module(wrapped_cmd, jid, job_path, write_fd):

    tmp_job_path = job_path + ".tmp"
    jobfile = open(tmp_job_path, "w")
    jobfile.write(json.dumps({ "started" : 1, "finished" : 0, "ansible_job_id" : jid }))
    jobfile.close()
    os.rename(tmp_job_path, job_path)
    jobfile = open(tmp_job_path, "w")
    result = {}

    outdata = ''
    try:
        cmd = shlex.split(wrapped_cmd)
        script = subprocess.Popen(cmd, shell=False, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Signal parent process that we have started, then close write side of pipe.
        os.write(write_fd, "OK")
        os.close(write_fd)
        (outdata, stderr) = script.communicate()
        result = json.loads(outdata)
        if stderr:
            result['stderr'] = stderr
        jobfile.write(json.dumps(result))

    except (OSError, IOError):
        e = sys.exc_info()[1]
        result = {
            "failed": 1,
            "cmd" : wrapped_cmd,
            "msg": str(e),
        }
        result['ansible_job_id'] = jid
        jobfile.write(json.dumps(result))

    except:
        result = {
            "failed" : 1,
            "cmd" : wrapped_cmd,
            "data" : outdata, # temporary notice only
            "msg" : traceback.format_exc()
        }
        result['ansible_job_id'] = jid
        jobfile.write(json.dumps(result))

    jobfile.close()
    os.rename(tmp_job_path, job_path)


####################
##      main      ##
####################
if __name__ == '__main__':

    if len(sys.argv) < 3:
        print(json.dumps({
            "failed" : True,
            "msg"    : "usage: async_wrapper <jid> <time_limit> <modulescript> <argsfile>.  Humans, do not call directly!"
        }))
        sys.exit(1)

    jid = "%s.%d" % (sys.argv[1], os.getpid())
    time_limit = sys.argv[2]
    wrapped_module = sys.argv[3]
    step = 5

    # setup job output directory
    jobdir = os.path.expanduser("~/.ansible_async")
    job_path = os.path.join(jobdir, jid)

    if not os.path.exists(jobdir):
        try:
            os.makedirs(jobdir)
        except:
            print(json.dumps({
                "failed" : 1,
                "msg" : "could not create: %s" % jobdir
            }))

    # Save the module and possible arguments so that they are available while
    # the forked child process is running the module.  Existing (non-async) code will attempt
    # to clean up the files if we don't rename them.  An alternative would be to make a copy
    # of them but rename should be quicker.
    saved_wrapped_module = job_path + ".module"
    os.rename(wrapped_module, saved_wrapped_module)

    if len(sys.argv) >= 5:
        argsfile = sys.argv[4]
        saved_argsfile = job_path + ".arguments"
        os.rename(argsfile, saved_argsfile)
        cmd = "%s %s" % (saved_wrapped_module, saved_argsfile)
    else:
        saved_argsfile = None
        cmd = saved_wrapped_module

    # Setup a pipe for parent/child synchronization so that child can let parent
    # know when it (child) has started and the parent can continue.
    read_fd, write_fd = os.pipe()

    # immediately exit this process, leaving an orphaned process
    # running which immediately forks a supervisory timing process

    try:
        pid = os.fork()
        if pid:
            # Notify the overlord that the async process started

            # Close write side of pipe - no longer needed in parent.
            os.close(write_fd)
            notice("Return async_wrapper task started.")
            print(json.dumps({ "started" : 1, "finished" : 0, "ansible_job_id" : jid, "results_file" : job_path }))
            sys.stdout.flush()
            # Read some dummy bytes ("OK") from child process.  Child process writes these once it has
            # started. Once child has started we can reliably continue (allowing attempted module and
            # arguments file removal).
            dummy_bytes_read = os.read(read_fd, 3)
            sys.exit(0)
        else:
            # The actual wrapper process

            # Close read side of pipe - no longer needed in child process.
            os.close(read_fd)

            # Daemonize, so we keep on running
            daemonize_self()

            # we are now daemonized, create a supervisory process
            notice("Starting module and watcher")

            sub_pid = os.fork()
            if sub_pid:
                # Watchdog does not need write side of pipe.
                os.close(write_fd)
                # the parent stops the process after the time limit
                remaining = int(time_limit)

                # set the child process group id to kill all children
                os.setpgid(sub_pid, sub_pid)

                notice("Start watching %s (%s)"%(sub_pid, remaining))
                while True:
                    # On FreeBSD (shippable automated test server), waitpid can sometimes
                    # return (0,-512) if waitpid is called quickly from parent.  So, only check
                    # the process id (waitpid_result[0]) for 0 value.  Do not rely on
                    # waitpid_result[1] here.
                    waitpid_result = os.waitpid(sub_pid, os.WNOHANG)
                    if remaining > 0 and waitpid_result[0] == 0:
                        notice("%s still running (%s)"%(sub_pid, remaining))
                        # could probably use higher resolution than 5 seconds here
                        time.sleep(step)
                        remaining = remaining - step
                    else:
                        break
                if remaining <= 0 and waitpid_result[0] == 0:
                    notice("Now killing %s"%(sub_pid))
                    os.killpg(sub_pid, signal.SIGKILL)
                    notice("Sent kill to group %s"%sub_pid)
                os.remove(saved_wrapped_module)
                if saved_argsfile is not None:
                    os.remove(saved_argsfile)
                notice("Done in kid B.")
                sys.exit(0)
            else:
                # the child process runs the actual module
                notice("Start module (%s)"%os.getpid())
                _run_module(cmd, jid, job_path, write_fd)
                notice("Module complete (%s)"%os.getpid())
                sys.exit(0)

    except SystemExit:
        # On python2.4, SystemExit is a subclass of Exception.
        # This block makes python2.4 behave the same as python2.5+
        raise

    except Exception:
        e = sys.exc_info()[1]
        notice("error: %s"%(e))
        print(json.dumps({
            "failed" : True,
            "msg"    : "FATAL ERROR: %s" % str(e)
        }))
        sys.exit(1)
